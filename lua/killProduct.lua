local exchange = ARGV[1]
local product = ARGV[2]
local key = exchange .. ":" .. product
local res = {}
local result = {}
local userName = ""


redis.call("DEL", exchange .. ":ORDERNUMBERS" )
redis.call("DEL", exchange .. ":FILLNUMBERS")
redis.call("DEL", key .. ":BESTQTY")
redis.call("DEL", key .. ":BESTPRICE")


local orders = redis.call("ZRANGE", exchange .. ":ORDERS", 0, -1)

for orderNumber, value in pairs(orders)
do
    -- table.insert(result,orderNumber)
    redis.call("DEL", exchange .. ":ORDER:" .. orderNumber)
    redis.call("DEL", exchange .. ":ORDER:" .. orderNumber .. ":QTY")
    redis.call("DEL", exchange .. ":ORDER:" .. orderNumber .. ":QTYLEFTOVER")
    redis.call("DEL", exchange .. ":ORDER:" .. orderNumber .. ":SIDE")
    redis.call("DEL", exchange .. ":ORDER:" .. orderNumber .. ":PRICE")
    redis.call("DEL", exchange .. ":ORDER:" .. orderNumber .. ":PRODUCT")

    userName = redis.call("GET", exchange .. ":USER:ORDER:" .. orderNumber)
    if (userName) then
        redis.call("DEL", exchange .. ":USER:ORDER:" .. orderNumber)
        redis.call("DEL", exchange .. ":" .. userName .. ":ORDER:" .. orderNumber)
        redis.call("DEL", exchange .. ":" .. userName ..":ORDERS")
    end
end

local depthB = redis.call("ZRANGE", key .. ":DEPTH:B", 0, -1)
for levelB, valueB in pairs(depthB)
do
    redis.call("DEL", key .. ":LEVELQTY:B:" .. valueB)
    table.insert(result,key .. ":LEVELQTY:S:" .. valueB)
    redis.call("DEL", key .. ":ORDERSINLEVEL:B:" .. valueB)
end

local depthS = redis.call("ZRANGE", key .. ":DEPTH:S", 0, -1)
-- table.insert(result,key .. ":DEPTH:S")
for levelS, valueS in pairs(depthS)
do
    redis.call("DEL", key .. ":LEVELQTY:S:" .. valueS)
    table.insert(result,key .. ":LEVELQTY:S:" .. valueS)
    redis.call("DEL", key .. ":ORDERSINLEVEL:S:" .. valueS)
end

redis.call("DEL", key .. ":DEPTH:S")
redis.call("DEL", key .. ":DEPTH:B")


local fills = redis.call("ZRANGE", exchange .. ":FILLS", 0, -1)
for fill, value in pairs(fills)
do
    redis.call("DEL", exchange .. ":FILL:" .. fill)
end

redis.call("DEL", exchange .. ":FILLS")
redis.call("DEL", exchange .. ":ORDERS")

return result