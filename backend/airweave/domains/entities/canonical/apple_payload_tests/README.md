# Native acquisition envelope tests

`fixtures/message-swift.json` and `note-swift.json` were produced by the actual
Swift `MessageObservation.encodedPayload()` and `NoteObservation.encodedPayload()`
implementations in the Apple source feature worktree. `NativeEnvelopes.swift`
constructs only synthetic rows: native Int64 `9007199254740993`, Unicode text and
bytes `00 ff c3 a9`. No provider database is opened by that generator.

`contact-swift.json` was produced by `ContactObservation.encodedPayload()` from a
synthetic CNMutableContact in AppleContactsTests. Its identifier is a framework
identifier generated for that synthetic object, not a fetched contact. Re-export:

```sh
frontend/desktop/native/apple-sources/Tests/AppleContactsTests/run.sh \
  --payload-output /absolute/path/contact-swift.json
```

The Notes body's bytes deliberately exercise binary fidelity; they are not a
valid compressed Notes protobuf. Projection tests must provide an actual generated
Notes body. `apple_preparation/tests/fixtures/` contains licensed native-format
Notes body examples and its tests demonstrate schema-based fixture generation.

Tests validate actual Swift Codable tagged values, strict envelope versions,
Int64 and binary preservation, locked-content withholding and malformed data.
`validate_device_original` returns a parsed value without changing the supplied
original. Admission separately compares `.native_id` with its canonical identity;
local lock/deletion observations require the appropriate visibility withdrawal.
