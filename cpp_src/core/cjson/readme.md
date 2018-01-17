


`CJSON` (Compact JSON) is internal reindexer binary format for transparing represent JSON data.
Each field of CJSON is encoded to `ctag` - varuint, which encodes type and name of field, and `data` binary representation of field data in format dependent of type.

## Ctag format

| Bits | Field | Description |
|------|-------------|-------------------------------------------------------------------------|
| 3 | TypeTag | Type of field. One of TAG_XXX  |
| 12 | NameIndex | Index of field's name in names dictionary. 0 - empty name |
| 6 | FieldIndex | Field index reference in internal reindexer payload. 0 - no reference.  |

## Ctag type tag

| Name | Value | Description |
|------------|-------|---------------------------------|
| TAG_VARINT | 0 | Data is number in varint format |
| TAG_DOUBLE | 1 | Data is number in double format |
| TAG_STRING | 2 | Data is string with varint length |
| TAG_ARRAY | 3 | Data is array of elements |
| TAG_BOOL | 4 | Data is bool |
| TAG_NULL | 5 | Null |
| TAG_OBJECT | 6 | Data is object |
| TAG_END | 7 | End of object |


## Arrays

Arrays can be stored in 2 different ways:

- homogenius array, with all elements of same type
- mixed array, with elements of various types

### Atag - array tag format

Atag is 4 byte int, which encodes type and count elemnts in array

| Bits | Field | Description |
|------|-------------|-------------------------------------------------------------------------|
| 24 | Count | Count of elements in array  |
| 3 | TypeTag | Type of array's elements. If TAG_OBJECT, than array is mixed, and each element contains individual ctag|

## Record format

````
record := 
  ctag := (TAG_OBJECT,name)
    [field := 
      ctag := (TAG_VARINT,name) data := <varint> |
      ctag := (TAG_DOUBLE,name) data := <8 byte double> |
      ctag := (TAG_BOOL,name) data := <1 byte: 0 - False, 1 - True> |
      ctag := (TAG_STRING,name) data := <varint(string length)>, <char array> |
      ctag := (TAG_NULL,name) |
      ctag := (TAG_ARRAY,name) 
        data := atag := TAG_OBJECT|TAG_VARINT|TAG_DOUBLE|TAG_BOOL, count) 
        array := [ctag := TAG_XXX] <data>,[[ctag := TAG_XXX>]<data>]] ... |
      ctag (TAG_OBJECT,name)> 
        [subfield := field]
      ...
      ctag := (TAG_END)
    ]
    ...
  ctag := (TAG_END)
````

## CJSON pack format

| # | Field                      | Description                                                                |
|---|----------------------------|----------------------------------------------------------------------------|
| 1 | Offset to names dict | Offset to names dictionary. 0 if there are no new names dictionary in pack |
| 2 | Records                    | Tree of records. Begins with TAG_OBJECT, end TAG_END                       |
| 3 | Names dictionary           | Dictionary of field names                                                  |
```
names_dictionary := 
  <varint(start_index)>
  <varint(count)>
  [[<varint(string length)>, <name char array>],
   [<varint(string length)>, <name char array>],
   ...
  ]
```

## Example of CJSON

```json
{
    "name": "Hello",
    "year": 2010,
    "articles": [1,2,3,4,5],
    "info": {
        "name" : "Info"
    }
}
```
```
(TAG_OBJECT)                                    06 
  (TAG_STRING,1) 5,"Hello"                      0A 05 48 65 6C 6C 6F
  (TAG_VARINT,2) 2010                           10 B4 1F
  (TAG_ARRAY,3) (TAG_VARINT,5) 1 2 3 4 5        1B 05 00 00 00 01 02 03 04 05
  (TAG_OBJECT,4)                                26
     (TAG_STRING,1) 4,"Info"                    0A 04 49 6E 66 6F
  (TAG_END)                                     07
(TAG_END)                                       07
```
