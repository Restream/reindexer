
dbname = 'test'
nsname = 'test_items'
depth = 1
items_path = '/api/v1/db/' .. dbname .. '/namespaces/' .. nsname .. '/items'
query_path = '/api/v1/db/' .. dbname .. '/query'

math.randomseed(os.time())

init = function (args)
	depth = tonumber(args[1]) or 1
end


function generate_test_item()
	str = '{"id":' .. math.random (1,2000000)
    local fCnt = math.random(10)
    for i=1,fCnt,1 do
		str = str .. ',"field' .. math.random(1,4090) .. '":'..math.random(1000000, 2000000)
	end
	str = str .. '}'
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
	[1] = function()
		return wrk.format('PUT', items_path, nil, generate_test_item())
	end,

	[2] = function()
		return wrk.format('POST', items_path, nil, generate_test_item())
	end,

	[3] = function()
		return wrk.format('DELETE', items_path, nil, generate_test_item())
	end,

	[4] = function() -- GET select items
		local limit = math.random(100)
		local offset = math.random(29900)
		local q = url_encode(string.format('select * from ' .. nsname .. ' limit %i offset %i', limit, offset))
		req = wrk.format('GET', query_path .. '?q='.. q, nil, nil)
		return req
	end
}

request = function ()
   local r = {}
   for i=1,depth do
      r[i] = switch[math.random(1,4)]()
   end
   return table.concat(r)
end

-- done = function(summary, latency, requests)
-- 	io.write("------------------------------\n")
-- 	io.write(string.format("Total completed: %i \n", summary.requests))
-- 	io.write(string.format("Error requests: %i \n", summary.errors.status))
-- end
