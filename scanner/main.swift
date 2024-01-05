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
    let name: UnsafeRawBufferPointer
    init(min: Float, max: Float, count: Int, sum: Float, name: UnsafeRawBufferPointer) {
        self.min = min
        self.max = max
        self.count = count
        self.sum = sum
        self.name = name
    }
}

struct DictionaryKey: Hashable {
    let hashValue: Int
    let cityName8Bytes: UInt64
    let bytes: UnsafeRawBufferPointer
    func hash(into hasher: inout Hasher) {
        withUnsafeBytes(of: hashValue) { rawBytes in
            hasher.combine(bytes: rawBytes)
        }
    }
    static func == (lhs: Self, rhs: Self) -> Bool {
        if rhs.hashValue != lhs.hashValue || rhs.cityName8Bytes != lhs.cityName8Bytes  {
            return false
        }
        let rhsCount = rhs.bytes.count
        let lhsCount = lhs.bytes.count
        if (rhsCount != lhsCount) {
            return false
        }
        if rhsCount < 8 {
            return true
        }
        for i in 0..<(rhsCount - 8) {
            if lhs.bytes[i] != rhs.bytes[i] {
                return false
            }
        }
        return true
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
let splits = numberOfCores
for i in 0..<splits {
    var end  =  data.count / splits * (i + 1)
    while (data[end] != newline) {
        end += 1
    }
    datas.append((start, end))
    start = end + 1
}

let operationQueue = OperationQueue()
operationQueue.qualityOfService = .userInitiated
operationQueue.maxConcurrentOperationCount = numberOfCores
func block(subdata: (Int,Int)) {
    let byCityThreaded = SimpleHashMap(capacity: 10240)
    
    data.withUnsafeBytes { fullPtr in
        guard let subrangeStart = fullPtr.baseAddress?.advanced(by: subdata.0),
              subdata.1 <= fullPtr.count else {
            fatalError("Subrange is out of bounds")
        }
        let bytes = UnsafeRawBufferPointer(start: subrangeStart, count: subdata.1 - subdata.0)
        var pos = 0
        var byte = bytes[pos]

        while pos < bytes.count {

            var cityNameHashCode = FNV_offset_basis
            var cityName8Bytes = 0 as UInt64
            let cityNameStart = pos
            
            while  byte != semicolon  {
                cityNameHashCode = (cityNameHashCode ^ Int(byte)) &* FNV_prime
                cityName8Bytes = cityName8Bytes << 8 | UInt64(byte)
                pos = pos &+ 1
                byte = bytes[pos]
            }
            let cityNameBytes = UnsafeRawBufferPointer(start: bytes.baseAddress!.advanced(by: cityNameStart), count: pos - cityNameStart)
            
            pos = pos &+ 1
            byte = bytes[pos]

            /*var cityNameBytes = [UInt8](repeating: 0, count: cityNameEnd - cityNameStart)
            cityNameBytes.withUnsafeMutableBytes { cityNameBytesPtr in
                cityNameBytesPtr.copyMemory(from: cityNamePtr)
            }*/
            
            //var cityNameString = String(bytes: cityNameBytes, encoding: .utf8)
            var cityValue = 0 as Int
            var valueSign = 1
            if true {
                if byte == minus {
                    valueSign = -1;
                } else {
                    cityValue = Int(byte - zero)
                }
            }
            pos = pos &+ 1
            byte = bytes[pos]
            while byte != newline  {

                if (byte != point) {
                    let val = byte - zero
                    cityValue = cityValue * 10 + Int(val)
                }
                pos = pos &+ 1
                byte = bytes[pos]
            }
            pos = pos &+ 1
            byte = bytes[pos]
            let value = Float(cityValue * valueSign) / 10
            
            
            let cityName = DictionaryKey(hashValue: cityNameHashCode, cityName8Bytes: cityName8Bytes, bytes: cityNameBytes)
            let hashIndex = byCityThreaded.find(key: cityName)
            if let statistic = byCityThreaded.valueAtIndex(index: hashIndex) {
                statistic.max = max(statistic.max, value);
                statistic.min = min(statistic.min, value);
                statistic.count += 1
                statistic.sum += value
            } else {
                byCityThreaded.insertAtIndex(index: hashIndex, key: cityName, value: Statistic(min: value, max: value, count: 1, sum: value, name: cityNameBytes))
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
for subdata in datas {
    operationQueue.addOperation {
      block(subdata: subdata)
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
    func valueAtIndex(index: Int) -> Statistic? {
        return _values[index]
    }
    func insertAtIndex(index: Int, key: DictionaryKey, value: Statistic) {
        _values[index] = value
        _keys[index] = key
    }
    
  
    
}

