--  wrk.method = "POST"
--  wrk.body   = "q=select%20*%20from%20test_item%20where%20id%3d10"
--  wrk.headers["Content-Type"] = "application/x-www-form-urlencoded"

init = function(args)

   depth = tonumber(args[1]) or 1

   local r = {}
   for i=1,depth do
      r[i] = wrk.format()
   end
   req = table.concat(r)
end

request = function()
   return req
end
