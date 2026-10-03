import Foundation
let root = URL(fileURLWithPath: CommandLine.arguments[1], isDirectory: true)
let text = "Hello — مرحبا — हिन्दी — 👩🏽‍💻"
let objects: [(String, Any)] = [
    ("attributed-body.bin", NSAttributedString(string: text)),
    ("mutable-attributed-body.bin", NSMutableAttributedString(string: text)),
    ("unsupported-string-root.bin", NSString(string: text))
]
for (name, object) in objects {
    try NSArchiver.archivedData(withRootObject: object).write(to: root.appendingPathComponent(name))
}
