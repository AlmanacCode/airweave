import Foundation
@testable import AppleSources
@testable import AppleNotes
let output = URL(fileURLWithPath: CommandLine.arguments[1])
let large: Int64 = 9_007_199_254_740_993
let original = Data([0, 255, 195, 169])
let message = MessageObservation(guid: "fixture-guid", message: AppleSources.NativeRow(rowID: large, fields: ["guid": .text("fixture-guid"), "text": .text("مرحبا"), "largeNativeColumn": .integer(large), "unknownFutureColumn": .blob(original)]), sender: nil, chats: [], participants: [], attachments: [], chatMemberships: [])
try message.encodedPayload().write(to: output.appendingPathComponent("message-swift.json"))
let note = NoteObservation(note: NotesRow(primaryKey: large, fields: ["Z_PK": .integer(large), "ZIDENTIFIER": .text("fixture-note"), "ZISPASSWORDPROTECTED": .integer(0), "ZMARKEDFORDELETION": .integer(0), "unknownFutureColumn": .blob(original)]), account: nil, folder: nil, attachments: [], compressedBody: original, fidelity: .compressedBodyUndecoded)
try note.encodedPayload().write(to: output.appendingPathComponent("note-swift.json"))
print("Synthetic Swift acquisition envelopes written; no provider read.")
