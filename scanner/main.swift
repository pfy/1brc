//
//  main.swift
//  scanner
//
//  Created by David Gunzinger on 03.01.2024.
//

import Foundation

class Statistic {
    var min: Float;
    var max: Float;
    var count: Int;
    var sum: Float;
    let name: [UInt8]
    init(min: Float, max: Float, count: Int, sum: Float, name: [UInt8]) {
        self.min = min
        self.max = max
        self.count = count
        self.sum = sum
        self.name = name
    }
}

struct DictionaryKey: Hashable {
    let hashValue: Int
    let bytes: [UInt8]
    func hash(into hasher: inout Hasher) {
        withUnsafeBytes(of: hashValue) { rawBytes in
            hasher.combine(bytes: rawBytes)
        }
    }
    static func == (lhs: Self, rhs: Self) -> Bool {
        if rhs.hashValue == lhs.hashValue && lhs.bytes.count == rhs.bytes.count && rhs.bytes.count < 8 {
            return true
        }
        return lhs.bytes == rhs.bytes
    }
}




let path = "/Users/pfy/Devel/1brc/measurements.txt"
let newline = "\n".data(using: .utf8)![0]
let semicolon = ";".data(using: .utf8)![0]
let zero = "0".data(using: .utf8)![0]
let point = ".".data(using: .utf8)![0]
let minus = "-".data(using: .utf8)![0]

var byCity = [DictionaryKey: Statistic]()
var byCityLock = NSRecursiveLock()


let data = try! Data(contentsOf: URL(fileURLWithPath: path), options: .alwaysMapped)
let numberOfCores = ProcessInfo.processInfo.activeProcessorCount

var datas = [] as [(Int,Int)]

var start = 0
for i in 0..<numberOfCores {
    var end  =  data.count / numberOfCores * (i + 1)
    while (data[end] != newline) {
        end += 1
    }
    datas.append((start, end))
    start = end + 1
}

let operationQueue = OperationQueue()

for subdata in datas {
    operationQueue.addOperation {
        var byCityThreaded = [DictionaryKey: Statistic]()
        byCityThreaded.reserveCapacity(1024)
        
        data.withUnsafeBytes { fullPtr in
            guard let subrangeStart = fullPtr.baseAddress?.advanced(by: subdata.0),
                  subdata.1 <= fullPtr.count else {
                fatalError("Subrange is out of bounds")
            }
            let bytes = UnsafeRawBufferPointer(start: subrangeStart, count: subdata.1 - subdata.0)

            var iterator = bytes.makeIterator()
            var cityNameBytes = [] as [UInt8]
            while true {
                cityNameBytes.removeAll(keepingCapacity: true)

                var cityNameHashCode = 0
                while let byte = iterator.next()   {
                    if byte == semicolon {
                 
                        break
                    }                    
                    cityNameHashCode = (31 &* cityNameHashCode) &+ (Int(byte))
                    cityNameBytes.append(byte)
                }
                
                // var cityNameString = String(bytes: cityNameBytes, encoding: .utf8)
                var cityValue = 0 as Int
                var valueSign = 1
                if let byte = iterator.next() {
                    if byte == minus {
                        valueSign = -1;
                    } else {
                        cityValue = Int(byte - zero)
                    }
                } else {
                    break
                }
                while let byte = iterator.next()  {
                    if byte == newline {
                        break;
                    }
                    
                    if (byte != point) {
                        let val = byte - zero
                        cityValue = cityValue * 10 + Int(val)
                    }
                }
                
                let value = Float(cityValue * valueSign) / 10
                let cityName = DictionaryKey(hashValue: cityNameHashCode, bytes: cityNameBytes)
                //byCityLock.withLock {
                if let statistic = byCityThreaded[cityName] {
                    statistic.max = max(statistic.max, value);
                    statistic.min = min(statistic.min, value);
                    statistic.count += 1
                    statistic.sum += value
                } else {
                    byCityThreaded[cityName] = Statistic(min: value, max: value, count: 1, sum: value, name: cityNameBytes)
                }
            }
            byCityLock.withLock {
                byCity = byCity.merging(byCityThreaded, uniquingKeysWith: { statistic, statistic2 in
                    statistic.max = max(statistic.max, statistic2.max)
                    statistic.min = min(statistic.min, statistic2.min)
                    statistic.count = statistic.count + statistic2.count
                    statistic.sum = statistic.sum + statistic2.sum
                    return statistic
                })
            }
        }
    }
}

operationQueue.waitUntilAllOperationsAreFinished()

print("All tasks completed")
let output = byCity.values.map({ value in
    return (String(bytes: value.name, encoding: .utf8)!, value)
}).sorted(by: { a, b in
    return a.0 < b.0
}).map{ data in
    let statistics = data.1
    return  String(format: "%@=%.1f/%.1f/%.1f", data.0, statistics.min, statistics.sum / Float(statistics.count), statistics.max)
}.joined(separator: ", ")
print("{\(output)}")
