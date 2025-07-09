# coding=utf-8

import yaml
import json
import re
import sys


def bool_constructor(self, node):
    return self.construct_scalar(node)


yaml.Loader.add_constructor(u'tag:yaml.org,2002:bool', bool_constructor)
yaml.SafeLoader.add_constructor(u'tag:yaml.org,2002:bool', bool_constructor)


def ParseRef(ref, typeDict, allTypeDict, propDict, level):
    global RefSubLevel
    splitref = ref.split("/")
    length = len(splitref)
    if length < 2:
        raise ValueError("Incorrect ref "+ref)
    if splitref[0] != "#":
        raise ValueError("Incorrect ref "+ref)
    refObj = server_yml["components"]["schemas"][splitref[-1]]
    if splitref[-1] in typeDict:
        if typeDict[splitref[-1]].counter < RefSubLevel:
            typeDict[splitref[-1]].counter += 1
            typeDict2 = dict(typeDict)
            GetParentType(typeDict2, allTypeDict,
                          refObj, propDict, level+"    ")
        else:
            propDict["type"] = "object"
            propDict["additionalProperties"] = True
    else:
        typeDict2 = dict(typeDict)
        typeDict2[splitref[-1]] = TypeData(1, refObj)
        GetParentType(typeDict2, allTypeDict, refObj, propDict, level+"    ")
    if splitref[-1] not in allTypeDict:
        allTypeDict[splitref[-1]] = refObj


def GetParentType(typeDict, allTypeDict, Obj, jsonObj, level):
    if "type" in Obj:
        if Obj["type"] == "object":
            jsonObj["type"] = "object"
            if "properties" in Obj and len(Obj["properties"]) != 0:
                if "required" in Obj:
                    jsonObj["required"] = Obj["required"]
                jsonObj["additionalProperties"] = False
                jsonObj["properties"] = {}
                Propertyes = Obj["properties"]
                for k in Propertyes:
                    jsonObj["properties"][k] = {}
                    GetParentType(typeDict, allTypeDict,
                                  Propertyes[k], jsonObj["properties"][k], level+"    ")
            else:
                jsonObj["additionalProperties"] = True
        elif Obj["type"] == "array":
            jsonObj["type"] = "array"
            itemsObj = Obj["items"]
            jsonObj["items"] = {}
            GetParentType(typeDict, allTypeDict, itemsObj,
                          jsonObj["items"], level+"    ")
        else:  # simpleType
            jsonObj["type"] = Obj["type"]
    elif "$ref" in Obj:
        ref = Obj["$ref"]
        if RefSubLevel == 0:
            refEmpty = {}
            jsonObj["$ref"] = ref
            ParseRef(ref, typeDict, allTypeDict, refEmpty, level)
        else:
            ParseRef(ref, typeDict, allTypeDict, jsonObj, level)


class TypeData():
    def __init__(self, counter, objectYml):
        self.counter = counter
        self.object = objectYml


def Generate():
    with open("server.yml", 'r') as stream:
        try:
            global server_yml
            server_yml = yaml.safe_load(stream)
            Query = server_yml["components"]["schemas"]["Query"]
            typeDict = {}
            typeDict["Query"] = TypeData(1, Query)
            allTypeDict = {}
            allTypeDict["Query"] = Query
            jsonEmpty = {}
            GetParentType(typeDict, allTypeDict, Query, jsonEmpty, "")

            schema = {}
            if RefSubLevel == 0:
                schema["$ref"] = "#/components/schemas/Query"
            else:
                allDummy = {}
                typeDummy = {}
                GetParentType(typeDummy, allDummy,
                              allTypeDict["Query"], schema, "")

            schema["definitions"] = {}
            for tpName in allTypeDict:
                jsonObj = {}
                allDummy = {}
                typeDummy = {}
                GetParentType(typeDummy, allDummy,
                              allTypeDict[tpName], jsonObj, "")
                schema["definitions"][tpName] = jsonObj


            if len(sys.argv) > 1:
                queryJsonFileName=sys.argv[1]
                with open(queryJsonFileName, "w") as json_file:
                #                json_file.write('static const std::string kQueryJson = R"#(')
                    json.dump(schema, json_file, indent=4)
#                json_file.write(')#";')
            with open("../../core/query/dsl/query.json.h", "w") as json_h_file:
                jsonstr = json.dumps(schema).encode()
                json_h_file.write('inline const std::string kQueryJson = {')

                for c in jsonstr:
                    v = hex(c)
                    json_h_file.write(v+',')
                json_h_file.write('};')
        except yaml.YAMLError as exc:
            print(exc)


RefSubLevel = 3
Generate()
