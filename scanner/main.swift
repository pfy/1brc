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
    init(min: Float, max: Float, count: Int, sum: Float) {
        self.min = min
        self.max = max
        self.count = count
        self.sum = sum
    }
}

print("Hello, World!")
let path = "/Users/pfy/Devel/1brc/measurements.txt"
let bufferSize = 1024 * 1024 * 16
let newline = "\n".data(using: .utf8)![0]
let semicolon = ";".data(using: .utf8)![0]
let zero = "0".data(using: .utf8)![0]
let point = ".".data(using: .utf8)![0]
let minus = "-".data(using: .utf8)![0]

var byCity = [[UInt8]: Statistic]()
var byCityLock = NSRecursiveLock()


let data = try! Data(contentsOf: URL(fileURLWithPath: path), options: .alwaysMapped)
let numberOfCores = ProcessInfo.processInfo.activeProcessorCount

var datas = [] as [Data]

var start = 0
for i in 0..<numberOfCores {
    var end  =  data.count / numberOfCores * (i + 1)
    while (data[end] != newline) {
        end += 1
    }
    datas.append(data.subdata(in: start..<end))
    start = end + 1
}

let operationQueue = OperationQueue()

for subdata in datas {
    operationQueue.addOperation {
        var byCityThreaded = [[UInt8]: Statistic]()
        
        subdata.withUnsafeBytes { bytes in
            var iterator = bytes.makeIterator()
        while true {
            var cityNameBytes = [] as [UInt8]
            while let byte = iterator.next()   {
                if byte == semicolon {
                    break
                }
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
            let cityName = cityNameBytes
            //byCityLock.withLock {
            if let statistic = byCityThreaded[cityName] {
                statistic.max = max(statistic.max, value);
                statistic.min = min(statistic.min, value);
                statistic.count += 1
                statistic.sum += value
            } else {
                byCityThreaded[cityName] = Statistic(min: value, max: value, count: 1, sum: value)
            }
        }
            byCityLock.withLock {
                for (cityName, value) in byCityThreaded {
                    if let statistic = byCity[cityName] {
                        statistic.max = max(statistic.max, value.max)
                        statistic.min = min(statistic.max, value.max)
                        statistic.count = statistic.count + value.count
                        statistic.sum = statistic.sum + value.sum
                    } else {
                        byCity[cityName] = value
                    }
                }
            }
        }
    }
}

operationQueue.waitUntilAllOperationsAreFinished()

print("All tasks completed")
let output = byCity.keys.map({ data in
    return (String(bytes: data, encoding: .utf8)!, data)
}).sorted(by: { a, b in
    return a.0 < b.0
}).map{ key in
    let statistics = byCity[key.1]!
    return  String(format: "%@=%.1f/%.1f/%.1f", key.0, statistics.min, statistics.sum / Float(statistics.count), statistics.max)
}.joined(separator: ", ")
print("{\(output)}")
