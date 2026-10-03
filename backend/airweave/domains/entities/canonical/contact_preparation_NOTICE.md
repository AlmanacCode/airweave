# Contacts phone preparation dependency

`phonenumberslite==9.0.40` is the Apache-2.0 Python port of Google's
libphonenumber. The lite distribution omits carrier/geocoder/timezone datasets.
The installed wheel retains its complete Apache-2.0 license in
`phonenumberslite-9.0.40.dist-info/licenses/LICENSE`.

Upstream implementation notices include Copyright (C) 2009-2011 The Libphonenumber
Authors. See the actual upstream
[license](https://github.com/daviddrysdale/python-phonenumbers/blob/dev/LICENSE)
and [project](https://github.com/daviddrysdale/python-phonenumbers).
No upstream implementation code or numbering data is copied into this module.

Preparation version `contacts-fields-v2` uses only explicit international `+`
input without a default region. National values remain raw and are marked
`region_required`. A future region preference requires actual source capture
policy and product input; names, locale and device country must not supply it.
Unicode decimal digits are mapped to ASCII with Python `unicodedata.decimal`
before whole-input validation and parsing, including extensions. Raw spellings
remain unchanged; the preparation descriptor records the Unicode database version.
Mixed decimal scripts are accepted, but superscripts, circled numbers and other
nondecimal numerics are not compatibility-normalized.
Numeric whole-input validation intentionally rejects prose, vanity spellings,
URI syntax and other unsupported forms instead of permissively extracting a number.
These spellings remain searchable as raw labeled values. E.164 and extension are
separate; parser validity is numbering-range classification, not ownership,
reachability or a person link. Formatting keys are discovery text, not identities.

Names, nickname, organization, emails and native labels retain original spelling,
case and Unicode. Individual observed contact cards are never merged. The exact
original remains canonical; prepared labeled text is `extracted_text` with a
versioned descriptor in the existing retained text manifest and read API. Existing
publications without the descriptor remain identifiable as old preparation; this
change does not silently reindex existing sources or bump their pipeline versions.
