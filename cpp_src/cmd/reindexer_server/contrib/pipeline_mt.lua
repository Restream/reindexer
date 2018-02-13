local cjson = require("cjson")

math.randomseed(os.time())
math.random(); math.random(); math.random()

function generate_test_item()
    local t = {}
    t["id"] = math.random(10001, 15000)
    local fCnt = math.random(10)
    for i=1,fCnt,1 do
        local fName = "field"..tostring(math.random(4090))
        t[fName] = math.random(1000000, 2000000)
    end
    return cjson.encode(t)
end

function url_decode(str)
  str = string.gsub (str, "+", " ")
  str = string.gsub (str, "%%(%x%x)",
      function(h) return string.char(tonumber(h,16)) end)
  str = string.gsub (str, "\r\n", "\n")
  return str
end

function url_encode(str)
  if (str) then
    str = string.gsub (str, "\n", "\r\n")
    str = string.gsub (str, "([^%w %-%_%.%~])",
        function (c) return string.format ("%%%02X", string.byte(c)) end)
    str = string.gsub (str, " ", "+")
  end
  return str
end

local switch = {
	[1] = function() -- PUT update item
		-- local id = math.random(10000)
		-- local randomNumber = 10000 + math.random(10000)
		-- local body = string.format('{ "id": %i, "randomNumber": %i}', id, randomNumber)
		return wrk.format('PUT', '/api/v1/namespaces/test_item/items', nil, generate_test_item())
	end,

	[2] = function() -- POST insert item
		-- local id = math.random(10000)
		-- local randomNumber = 20000 + math.random(10000)
		-- local body = string.format('{ "id": %i, "randomNumber": %i}', id, randomNumber)
		return wrk.format('POST', '/api/v1/namespaces/test_item/items', nil, generate_test_item())
	end,

	[3] = function() -- DELETE delete item
		-- local id = 1000000 + math.random(200000)
		-- local body = string.format('{ "id": %i}', id)
		return wrk.format('DELETE', '/api/v1/namespaces/test_item/items', nil, generate_test_item())
	end,

	[4] = function() -- GET select items
		local limit = math.random(100)
		local offset = math.random(29900)
		local q = url_encode(string.format('select * from test_item limit %i offset %i where id > 10', limit, offset))
		req = wrk.format('GET', '/api/v1/query?q='..q, nil, nil)
		return req
	end
}

request = function ()
	local req = switch[math.random(4)]
	return req()
end

done = function(summary, latency, requests)
	io.write("------------------------------\n")
	io.write(string.format("Total completed: %i \n", summary.requests))
	io.write(string.format("Error requests: %i \n", summary.errors.status))
end
