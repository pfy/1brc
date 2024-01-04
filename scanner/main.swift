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

/*let tst = SimpleHashMap(capacity: 10)
tst[DictionaryKey(hashValue: 123, bytes: [0xff,0xff])] = Statistic(min: 0, max: 0, count: 0, sum: 0, name: [0xff,0xff])
for elem in tst {
    print(elem)
}*/


let path = "/Users/pfy/Devel/1brc/measurements.txt"
let newline = "\n".data(using: .utf8)![0]
let semicolon = ";".data(using: .utf8)![0]
let zero = "0".data(using: .utf8)![0]
let point = ".".data(using: .utf8)![0]
let minus = "-".data(using: .utf8)![0]
let FNV_prime =  0x100000001b3 as Int
let FNV_offset_basis =  Int(bitPattern: 0xcbf29ce484222325)

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
        let byCityThreaded = SimpleHashMap(capacity: 1024)
        
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

                var cityNameHashCode = FNV_offset_basis
                while let byte = iterator.next()   {
                    if byte == semicolon {
                 
                        break
                    }                    
                    cityNameHashCode = (cityNameHashCode ^ Int(byte)) &* FNV_prime
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



class SimpleHashMap:  Collection, Sequence {
    private var _values: [Statistic?]
    private var _keys: [DictionaryKey?]
    private var _capacity: Int
    func index(after i: Int) -> Int {
        var n = i+1
        while n < _keys.count &&  _keys[n] == nil && n < _capacity {
            n += 1
        }
        return n
    }
    var startIndex: Int { return index(after: -1) }
    var endIndex: Int   { return _capacity }
    subscript(index: Int) -> Element {
           return (_keys[index]!,_values[index]!)
    }
    
    typealias Element = (DictionaryKey, Statistic)


    init(capacity: Int) {
        _capacity = capacity
        _values = Array(repeating: nil, count: capacity)
        _keys = Array(repeating: nil, count: capacity)
    }
    subscript(key: DictionaryKey) -> Statistic? { 
        get {
             let index = find(key: key)
            return _values[index]
            
        }
        set {
            let index = find(key: key)
                _values[index] = newValue
                _keys[index] = key

            
        }
    }

    func find(key: DictionaryKey) -> Int {
        let hash = (key.hashValue % _capacity + _capacity) % _capacity
        var distance = 1
        var index = hash
        while _keys[index] != nil && _keys[index] != key {
            index = (index + distance) % _capacity
            distance = distance * 2
        }
        return index
    }
  
    
}

