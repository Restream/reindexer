# Reindexer data storage format
 
Internally reindexer is processing documents in special objects, called `Payload`. Documents are stored splitted into 3 parts:

- untyped tuple of non-indexed fields;
- fixed-type struct of indexed fields;
- separate column storage binded to full scan indexes.

# Motivation

Main goal of documents splitting technick - get the best features from SQL, no-SQL and column databases.
The untyped tuple is gives applications capability to store any JSON objects as reindexer documents without data schema migration.
The fixed-type struct of indexed fields allows application and reindexer maintains consistent indexes and primary keys. Furthermore it helps to achive best performance for full-scan and join queries - their
performance are strongly depends on fields access time. 

Also there are optional column's storage is present. They are binded to full-scan indexes, and can speed up full-scan queries.

# Payload 

Payload is fixed-type struct of indexed fields. Internal Payload API is represented by 3 top level classes: 

- `PayloadValue` is shared copy on write fields structure. Size of structure is equal to sum of fields size + 8 bytes header;
- `PayloafType` is payload structure definition. Contains vector of fields (names, types, sizes, offsets);
- `PayloadIface` is template of payload control interface. Holds pointer to PayloadValue and PayloadType.  There are 2 instantiation: `Payload` - can modify PayloadValue; class `ConstPayload` - read only interface, can't modify PayloadValue.

Payload API is close to usual reflection API and contains methods `Set` and `Get` to manipulating fields
See [payloadiface.h](payloadiface.h) for details.


## PayloadValue structure 

| Field         | RefCount | Cap   | LSN | Field1 | ... | FieldN | Embedded Arrays |
|---------------|----------|-------|-----|--------|-----|--------|-----------------|
| Size in bytes | 4        | 4     | 8   | Vary   |     | Vary   | Vary            |


### Data format of fields

Trivial data types like int32, int64, double are stored in their native format. 

| Field         | Int Field | Int64 Field | Double Field | String Field |
|---------------|-----------|-------------|--------------|--------------|
| Size in bytes | 4         | 8           | 8            | 8            |


Strings are stored as `reindexer::p_string`, which is 8-byte weak pointer to string. Important, that `PayloadValue` itself does not owning string.


### p_string 

`p_string` is pointer wrapper, and uses 60-s and 61-st bits of pointer as string type tag. (It assumes, that x86_64 address space is 2^48 https://en.wikipedia.org/wiki/X86-64#Virtual_address_space_details)

| Tag | Pointed object                                            |
|-----|-----------------------------------------------------------|
| 0   | c null-terminated string                                  |
| 1   | 4-byte string len header, followed by string's char array |
| 2   | reindexer::key_string                                     |
| 3   | varint len header, followed by string's char array        |


### key_string
`reindexer::key_string` is derived from std::string, and in addition contains reference count and exported header with pointer + size fields

| Size in bytes          | Field                          |
|------------------------|--------------------------------|
| Vary <br> (depends on stl) | std::string                    |
| 8                      | exported pointer to string's<br> char array |
| 4                      | exported string len                     |
| 4                      | alignment                      |
| 8                      | ref counter                    |

Exported header is used for direct payload access from non c++ application (e.g. golang binding to reindexer)
Data in key_string is mutable.

### Embeded arrays in PayloadValue

Arrays are stored as ArrayHeader, which holds 4-byte start offset and 4-byte count of elements in array. 

| Field         | Array Field1<br>Offset+Count | FiendN | Array1<br> Element1 | .. | Array1<br>Element N |
|---------------|---------------------------|--------|------------------|----|------------------|
| Size in bytes | 4+4                       | Vary   | Vary             |    | Vary             |


### Data sample

```c++

struct
{
   int64_t f1 = 5;
   int f2[] = {6,7,8};
   int f3 = 10;
   string f4 = "abc";
}
```

| Field     | Size | Offset | Value        |
|-----------|------|--------|--------------|
| f1        | 8    | 0      | int64_t(5)   |
| f2.offset | 4    | 8      | uint32_t(28) |
| f2.count  | 4    | 12     | int32_t(3)   |
| f3        | 4    | 16     | int32_t(10)  |
| f4        | 8    | 20     | &string(abc) |
| f2[0]     | 4    | 28     | int32_t(6)   |
| f2[1]     | 4    | 32     | int32_t(7)   |
| f2[2]     | 4    | 36     | int32_t(8)   |


## Lifetime and ownership

`PayloadValue` behaviour is similar to `std::shared_ptr`. `PayloadValue` increments refcounter on copy, decrements refcounter in destructor and deletes holding structure, if refcounter is 0.
refcounter of `PayloadValue` is thread safe.

`PayloadValue` can be in 3 states:
- *Free*: data struct is not allocated
- *Non Shared*: data struct is exclusively owning by `PayloadValue`
- *Shared*: data struct is shared with other `PayloadValue`

`PayloadValue` does not allow modifications in shared state. Before any modification, code MUST create copy by calling `PayloadValue::Clone ()`.  
`Clone` will check state and if neccesary allocates new struct or creates exclusive copy.

`PayloadValue` does not owning it's strings by default. To control ownership of strings there are 2 methods `PayloadIface::AddRefStrings ()` and `PayloadIface::ReleaseStrings ()`. 

*IMPORTANT* `AddRefStrings` and `ReleaseStrings` *is not* works by RAII idiom, so code MUST itself control balance of calls `AddRefStrings` and `ReleaseStrings`. If code forgot to call `ReleaseStrings`, then memory will leak.

There are only 1 copy of `PayloadType` per 1 namespace. It's mutable and threadsafe.

## Untyped tuple storing

Untyped typle of nonidexed fields is stored in `CJSON` format in 1-st field (name "-tuple", type string) of payload.

## Typical usage

```c++


	PayloadType type = ns->payloadType_;
	PayloadValue value  = ns->items_[index];

	// Create control object
	ConstPayload payload(type, value);

	VariantArray keyRefs;

    // Dump all fields to stdout
    for (int i = 0; i < payload.NumFields(); i++) {
		auto &field = payload.Type().Field(i);
		printf("\n%s=", field.Name().c_str());
		for (auto &elem : payload.Get (i,keyRefs)) {
			printf("%s", Variant(elem).toString().c_str());
		}
	}

```
