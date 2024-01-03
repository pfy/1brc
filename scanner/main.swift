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
let newline = "\n".data(using: .utf8)!
let semicolon = ";".data(using: .utf8)!
var byCity = [String: Statistic]()

guard let handle = try? FileHandle(forReadingFrom: URL(fileURLWithPath: path)) else {
    print("can't open file")
    exit(1)
}
var leftover = Data()
while true {
    guard var data = try handle.read(upToCount: bufferSize) else {
        break
    }
    data = leftover + data
    var searchRange = 0..<data.count
    var newlineRange = data.range(of: newline, in: searchRange)
    while newlineRange != nil {
        let semicolonRange = data.range(of: semicolon, in: searchRange.lowerBound..<newlineRange!.lowerBound)
        guard let semicolonRange = semicolonRange else {
            print("no semicolon in line")
            exit(1)
        }
        
        let city = String(data: data.subdata(in: searchRange.lowerBound..<semicolonRange.lowerBound), encoding: .utf8)!
        let value = Float(String(data: data.subdata(in: semicolonRange.upperBound..<newlineRange!.lowerBound), encoding: .ascii)!)!
        if let statistic = byCity[city] {
            statistic.max = max(statistic.max, value);
            statistic.min = max(statistic.min, value);
            statistic.count += 1
            statistic.sum += value
        } else {
            byCity[city] = Statistic(min: value, max: value, count: 1, sum: value)
        }
        searchRange = newlineRange!.upperBound..<data.count
        newlineRange = data.range(of: newline, in: searchRange)
    }
    leftover = data.subdata(in: searchRange)
//    let parts = data.split(separator: newline)
    if  data.isEmpty {
        break;
    }
}
/*
for try await line in handle.bytes.lines {
    let parts = line.split(separator: ";")
    let city = String(parts[0])
    let value = Float(parts[1])!
    if let statistic = byCity[city] {
        statistic.max = max(statistic.max, value);
        statistic.min = max(statistic.min, value);
        statistic.count += 1
        statistic.sum += value
    } else {
        byCity[city] = Statistic(min: value, max: value, count: 1, sum: value)
    }
}*/



/*guard let data = try? Data(contentsOf: URL(filePath: path), options: .alwaysMapped) else {
    print("can't map data")
    exit(1)
}
let newlineData = "\n".data(using: .utf8)![0]
let semicolonData = ";".data(using: .utf8)![0]
let ranges = data.ranges(of: [newlineData, semicolonData])
exit(0)
*/
/*
for line in reader {
    let parts = line.split(separator: ";")
    let city = String(parts[0])
    let value = Float(parts[1].replacing("\n", with: ""))!
    if var statistic = byCity[city] {
        statistic.max = max(statistic.max, value);
        statistic.min = max(statistic.min, value);
        statistic.count += 1
        statistic.sum += value
    } else {
        byCity[city] = Statistic(min: value, max: value, count: 1, sum: value)
    }
    //print(">" + line.trimmingCharacters(in: .whitespacesAndNewlines))
}*/
let output = byCity.keys.sorted().map{ key in
    let statistics = byCity[key]!
    
    
    return "\(key)=\(statistics.min)/\(statistics.sum / Float(statistics.count))/\(statistics.max)"
}.joined(separator: ",")
print("{\(output)}")
exit(0)
