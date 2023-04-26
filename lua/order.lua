
local finals = ""
local fillsJsonTable = "" -- important because this is an edge variable. It gets set from outside the functions
local fillsCount = 0
local depthUpdates
local verbose = true

-- TESTING
local function display_fills()

    local displayFills = "\r======== FILLS ==========\r"
    local fillNumbers = redis.call("ZRANGE", "Insta:FILLS",0,-1)
    for key,value in pairs(fillNumbers) 
    do 
        displayFills = displayFills .. redis.call("GET", "Insta:FILL:" .. value) .. "\r"
    end
    return displayFills

end

-- TESTING
local function display_orders()

    local displayOrder = "\r======== ORDERS ==========\r"
    local orderNumbers = redis.call("ZRANGE", "Insta:ORDERS",0,-1)

    for key,value in pairs(orderNumbers) 
    do 
        displayOrder = displayOrder ..  redis.call("GET", "Insta:ORDER:" .. value)  .. "\r"
    end
    return displayOrder

end


-- TESTING
local function display_order_qty_leftovers()

    local displayOrderQty = "\r======== ORD QTYs ==========\r"
    local orderNumbers = redis.call("ZRANGE", "Insta:ORDERS",0,-1)

    for key,value in pairs(orderNumbers) 
    do 
        displayOrderQty = displayOrderQty .. "Order# " .. value .. " - Qty: " .. redis.call("GET", "Insta:ORDER:" .. value .. ":QTY") .. " / QtyLeft: " .. redis.call("GET", "Insta:ORDER:" .. value .. ":QTYLEFTOVER") .. "\r"
    end
    return displayOrderQty

end

local function cancel(exchange, orderNumber, user,timestamp)
    local side = redis.call("GET",exchange .. ":ORDER:" .. orderNumber .. ":SIDE")
    local price = redis.call("GET",exchange .. ":ORDER:" .. orderNumber .. ":PRICE")
    local product = redis.call("GET",exchange .. ":ORDER:" .. orderNumber .. ":PRODUCT")
    local qtyLeftinOrder = redis.call("GET",exchange .. ":ORDER:" .. orderNumber .. ":QTYLEFTOVER")
    local volInLevel = redis.call("GET",exchange .. ":" .. product .. ":LEVELQTY:" .. side .. ":" .. price)
    local oldOrderUser = redis.call("GET",exchange .. ":USER:ORDER:" .. orderNumber)
   -- local exchangeAdminUser = redis.call("GET", exchange .. ":ADMINUSER")
    local exchangeAdminUser = "admin"

--    redis.call('PUBLISH','debug', "****** CANCEL ****** qtyLeftInOrder: " .. tonumber(qtyLeftinOrder) .. "oldOrderUser: " .. oldOrderUser .. " user " .. user .. " admin " .. exchangeAdminUser)

    if ( (tonumber(qtyLeftinOrder) > 0 and tostring(oldOrderUser) == tostring(user)) or user == exchangeAdminUser ) then
        local cancelNumber = redis.call("INCRBY", exchange .. ":CANCELNUMBERS",1)
        redis.call("ZADD",exchange .. ":CANCELS", timestamp, cancelNumber)

        if (volInLevel == qtyLeftinOrder) then
            redis.call("DEL", exchange .. ":" .. product .. ":LEVELQTY:"  .. side .. ":" .. price) -- removes the level qty key
            redis.call("ZREM", exchange .. ":" .. product .. ":DEPTH:" .. side, price) -- removes LEVEL IN DEPTH: the level with this price in the depth knowing that that level was entirely composed of that order
        else  -- the level has more volume than the cancellation can remove
            redis.call("INCRBY", exchange .. ":" .. product .. ":LEVELQTY:" .. side .. ":" .. price, -1 * qtyLeftinOrder) -- remove the amount of volume in the level that was taken by the cancel

        end
        redis.call("ZREM", exchange .. ":" .. product .. ":ORDERSINLEVEL:" .. side .. ":" .. price,orderNumber) -- removes ORDER IN LEVEL: the order from the levels
        redis.call("SET", exchange .. ":ORDER:" .. orderNumber .. ":QTYLEFTOVER",0) -- zeroes out the leftover qty of the order
    end
end

local function createFill(exchange,NewOrderNumber, NewOrderSide, OldOrderNumber, qtyFilled, newUser, oldUser, oldPrice)
    fillsCount = fillsCount + 1
    fillsJsonTable = fillsJsonTable .. '{"orderNumber": "' .. OldOrderNumber .. '", "qty": "' .. qtyFilled .. '", "price": ' .. oldPrice .. ', "user": "' .. oldUser .. '"},'
    local fillNumber = tostring(redis.call('INCRBY',exchange .. ":FILLNUMBERS",1))
    redis.call('ZADD',exchange .. ":FILLS", fillNumber, fillNumber)
    if (NewOrderSide == "B") then
        redis.call("SET", exchange .. ":FILL:" .. fillNumber , '{"exchange":"' .. exchange .. '", "BuyerOrderNumber":"' .. NewOrderNumber .. '", "SellerOrderNumber":"' .. OldOrderNumber .. '", "qtyFilled":"' .. qtyFilled .. '", "seller":"' .. oldUser .. '", "buyer":"' .. newUser .. '", "price":"' .. oldPrice ..  '"}')
    else
        redis.call("SET", exchange .. ":FILL:" .. fillNumber , '{"exchange":"' .. exchange .. '", "BuyerOrderNumber":"' .. OldOrderNumber .. '", "SellerOrderNumber":"' .. NewOrderNumber .. '", "qtyFilled":"' .. qtyFilled .. '", "seller":"' .. newUser .. '", "buyer":"' .. oldUser .. '", "price":"' .. oldPrice ..  '"}')
    end
    return fillNumber
end



local function add_volume_to_depth(exchange, product, side, price, qty, number, timestamp)
    redis.call('ZADD',exchange .. ":" .. product .. ":DEPTH:" .. side, price,price)
    if (verbose) then
        redis.call('PUBLISH','verboselog', 'Adding to depth ' .. qty)
    end
    local newVol = redis.call('INCRBY', exchange .. ":" .. product .. ":LEVELQTY:" .. side .. ":" .. price, qty)
    if (verbose) then
        redis.call('PUBLISH','verboselog', 'New volume of this level is ' .. newVol)
    end
    depthUpdates = depthUpdates .. '{"qty":' .. newVol .. ', "price": ' .. price .. '}'
    redis.call('ZADD', exchange .. ":" .. product .. ":ORDERSINLEVEL:" .. side .. ":" .. price, timestamp, number)
    if (verbose) then -- can't have this after the return
         redis.call('PUBLISH','verboselog', 'CHANGED LEVEL: Side=' .. side .. ' Price=' .. price .. ' to qty=' .. newVol .. ' AND BROADCASTED ABOUT IT')
    end
    return newVol

end


local function init(order)
    -- provide a basic depth for testing
    redis.call('FLUSHALL')
    local initSide = "S"
    if (order.side == "S") then
        initSide = "B"
    else
        initSide = "S"
    end
    add_volume_to_depth(order.exchange, order.product, initSide, 93, 2, 1234, 1)
    redis.call('SET', exchange .. ":ORDER:1234:QTYLEFTOVER", 2)
    add_volume_to_depth(order.exchange, order.product, initSide, 94, 5, 4567, 2)
    redis.call('SET', exchange .. ":ORDER:4567:QTYLEFTOVER", 5)
    add_volume_to_depth(order.exchange, order.product, initSide, 95, 10, 8901, 3)
    redis.call('SET', exchange .. ":ORDER:8901:QTYLEFTOVER", 5)

end

local function getLowestPriceInDepth(depth)
    return redis.call('ZRANGE', order.exchange .. ":" .. order.product .. ":DEPTH:" .. depth.side,0,0)
end

local function reduce_volume_matched_orders_at_this_level(level, qtyLevelReduction, order)
-- level is just 1 price in the depth
-- order is the newOrder
-- qtyLevelReduction is the big volume that needs to be allocated to the many old orders in each level
-- the goal of this is solely to reduce the volume of the old orders

    if (verbose) then
        redis.call('PUBLISH','verboselog', '    ALSO ADDRESSING UNDERLYING ORDERS FOR THAT DEPTH THAT NEED REDUCING BY: ' .. qtyLevelReduction) 
    end

    local newOrderNumber = order.number
    local leftOver = tonumber(qtyLevelReduction) -- we save that separately so that we can lower it as we take volume from old orders

    local topOrderInLevelKey = level.exchange .. ":" .. level.product .. ":ORDERSINLEVEL:"  .. level.side .. ":" .. level.price
    local i = 0
    local oldOrderNumber = redis.call("ZRANGE", topOrderInLevelKey,i,i)[1]
    if (oldOrderNumber ~= nil) then
        local oldOrderQtyLeftKey = order.exchange .. ":ORDER:" .. oldOrderNumber .. ":QTYLEFTOVER"
        local oldOrderQtyLeft = tonumber(redis.call("GET",oldOrderQtyLeftKey))
        local oldOrderPriceKey = order.exchange .. ":ORDER:" .. oldOrderNumber .. ":PRICE"
        local oldOrderPrice = redis.call("GET",oldOrderPriceKey)
        local oldOrderUser = redis.call("GET",level.exchange .. ":USER:ORDER:" .. oldOrderNumber) 
        
        if (verbose) then
            redis.call('PUBLISH','verboselog', 'FOUND OLD ORDER THAT NEEDS REDUCING: Order#=' .. oldOrderNumber .. ' QtyLeft=' .. oldOrderQtyLeft .. ' price=' .. oldOrderPrice)
        end

    --    redis.call('PUBLISH','debug', "Order# " .. newOrderNumber .. " is trying to reduce level of price " .. level.price .. " that has leftOverQty of " .. oldOrderQtyLeft  .. " by a qty of " .. qtyLevelReduction .. " using order# " .. oldOrderNumber)

        while (leftOver > 0 and oldOrderNumber ~= nil) -- while we still have qty to deplete and we haven't explored ALL old orders
        do
            if (oldOrderQtyLeft>0) then -- NEEDS OPTIMIZATION: THERE IS NO GOOD REASON FOR THIS TO GO THROUGH ALL OLD ORDERS WITH ZERO QTY LEFT
                if (oldOrderQtyLeft > leftOver) then -- 12/17: REMOVED EQUAL SIGN on >= to >
                    if (verbose) then
                        redis.call('PUBLISH','verboselog', 'BUGGY LAND #1')
                    end
                    local oldOrderVol = redis.call("INCRBY",oldOrderQtyLeftKey,-1*leftOver) -- so consume some of the old order                    
                    createFill(level.exchange,newOrderNumber, order.side, oldOrderNumber, leftOver,order.user,oldOrderUser,oldOrderPrice)
                    leftOver = 0  -- consume all the volume of the new order
                    -- redis.call("ZREM",topOrderInLevelKey, oldOrderNumber) -- 12/17: REMOVED THIS
                    -- redis.call("SET",level.exchange .. ":" .. level.product .. ":LEVELQTY:"  .. level.side .. ":" .. level.price, 0 ) -- 12/17: REMOVED THIS
                    if (verbose) then
                        -- redis.call('PUBLISH','verboselog', 'BUGGY LAND #2')
                        redis.call('PUBLISH','verboselog', 'REDUCING OLD ORDER with qty=' .. oldOrderQtyLeft .. ' by ' .. -1*leftOver .. ' to ' .. oldOrderVol)
                        -- redis.call('PUBLISH','verboselog', 'BUGGY LAND #3')
                    end                    
                    --redis.call("PUBLISH","debug", "HONEY IM HERE!! " .. level.exchange .. ":" .. level.product .. ":" .. level.side .. ":" .. level.price)
                    -- i = i + 1 -- BAD
                else
                    -- redis.call("SET",oldOrderQtyLeftKey,0) -- the new order is bigger that its old match, so it zeroes out the match -- NEEDS OPTIMIZATION: THERE IS NO GOOD REASON FOR THIS TO GO THROUGH ALL OLD ORDERS WITH ZERO QTY LEFT
                    redis.call("ZPOPMIN", topOrderInLevelKey) -- this is equivalent to i = i + 1
                    redis.call("ZREM",topOrderInLevelKey, oldOrderNumber) -- 12/17: ADDED THIS
                    redis.call("SET",oldOrderQtyLeftKey,0) -- recent
                    createFill(level.exchange,newOrderNumber, order.side, oldOrderNumber, oldOrderQtyLeft, order.user, oldOrderUser,oldOrderPrice)
                    leftOver = leftOver - oldOrderQtyLeft                    
                    if (verbose) then
                        -- redis.call('PUBLISH','verboselog', 'BUGGY LAND #4')
                        redis.call('PUBLISH','verboselog', 'REMOVING OLD ORDER#=' .. oldOrderNumber .. ' leaving us with ' .. leftOver .. ' qty to remove from this level s orders')
                    end                    

    --                redis.call('PUBLISH','debug',"Old order: " .. oldOrderNumber .. " - New Order: " .. newOrderNumber .. " - leftOver: " .. leftOver .. " oldOrderQtyLeft: " .. oldOrderQtyLeft)

                end
            end
    -- end 12/17 REMOVED OOPSIES CHANGE
            -- and now we move onto the next order inside the level
                i = i + 1 -- THIS IS AN INSANE UNCHECKED CHANGE from where it was above
                oldOrderNumber = redis.call("ZRANGE", topOrderInLevelKey,i,i)[1]
                if (oldOrderNumber ~= nil) then
                     oldOrderQtyLeft = tonumber(redis.call("GET",oldOrderQtyLeftKey))
                     oldOrderPrice = redis.call("GET",oldOrderPriceKey)
                     oldOrderUser = redis.call("GET",level.exchange .. ":USER:ORDER:" .. oldOrderNumber)
                     oldOrderQtyLeftKey = order.exchange .. ":ORDER:" .. oldOrderNumber .. ":QTYLEFTOVER"
                     oldOrderQtyLeft = tonumber(redis.call("GET",oldOrderQtyLeftKey))
                
                     if (verbose) then
                        redis.call('PUBLISH','verboselog', '2-FOUND OLD ORDER THAT NEEDS REDUCING: Order#=' .. oldOrderNumber .. ' QtyLeft=' .. oldOrderQtyLeft .. ' price=' .. oldOrderPrice)
                     end

                end
        end -- of loop 12/17 ADDED
    end


    -- now one just has to iterate through the orders in this level
end

local function createOrder(order)
    if (verbose) then
        redis.call('PUBLISH','verboselog', 'CREATING ORDER')
    end
    local orderNumber = tostring(redis.call('INCRBY',order.exchange .. ":ORDERNUMBERS",1))

    redis.call('ZADD',order.exchange .. ":ORDERS", order.timestamp, orderNumber)
    redis.call("ZADD",order.exchange .. ":" .. order.user .. ":ORDERS", order.timestamp, orderNumber) -- orders by user
    redis.call("SET", order.exchange .. ":ORDER:" .. orderNumber .. ":PRICE", order.price)
    redis.call("SET", order.exchange .. ":ORDER:" .. orderNumber .. ":SIDE", order.side)
    redis.call("SET", order.exchange .. ":ORDER:" .. orderNumber .. ":PRODUCT", order.product)
    redis.call("SET", order.exchange .. ":ORDER:" .. orderNumber .. ":QTY", order.qty)
    redis.call("SET", order.exchange .. ":ORDER:" .. orderNumber .. ":QTYLEFTOVER", order.qty)
    redis.call("SET", order.exchange .. ":USER:ORDER:" .. orderNumber,order.user)
    redis.call("SET", order.exchange .. ":ORDER:" .. orderNumber, '{"exchange" : "' .. order.exchange .. '", "product" : "' .. order.product .. '", "side" : "' .. order.side .. '", "price" : ' .. order.price .. ', "qty" : ' .. order.qty .. ', "timestamp" : ' .. order.timestamp .. ', "user" : "' .. order.user .. '"}')
    redis.call("SET", order.exchange .. ":" .. order.user .. ":ORDER:" .. orderNumber, '{"exchange" : "' .. order.exchange .. '", "product" : "' .. order.product .. '", "side" : "' .. order.side .. '", "price" : ' .. order.price .. ', "qty" : ' .. order.qty .. ', "timestamp" : ' .. order.timestamp .. ', "user" : "' .. order.user .. '"}')
    if (verbose) then
        redis.call('PUBLISH','verboselog', 'LOADED ATTRIBUTES IN REDIS AND ASSIGNED ORDER NUMBER: ' .. orderNumber)
    end
    return orderNumber
end


local function process_order(order)
    
    if (verbose) then
    redis.call('PUBLISH','verboselog', 'PROCESSING ' .. order.side .. ' ORDER: Price=' .. order.price .. ' Qty=' .. order.qty)
    end

 -- tries to match an order against the various prices in a depth
 
    -- 1st step: finds the one cheapest sell price

    -- let's define the depth we're going to explore and create a level object for each level we'll inspect
    order.number = createOrder(order)
    -- TESTING
    finals = finals .. "\r\rAn order was entered: " .. order.side .. " " .. order.qty .. " @ " .. order.price .. " (#" .. order.number .. ")\r\r"

    local depth = {exchange = order.exchange, product = order.product, side =""}
    if (order.side == "S") then
        depth.side = "B"
    else
        depth.side = "S"
    end
    local depthKey = depth.exchange .. ":" .. depth.product .. ":DEPTH:" .. depth.side
    
    local level = {exchange = depth.exchange, product = depth.product, side = depth.side, price = 0, qty = 0}

    local overflow = 0
            
    local leftOverQty = order.qty
    local newVol = 0
    if (depth.side == "S") then
        depthUpdates = depthUpdates .. '"sells": [  '
        -- now let's find the 1 level within that depth with the cheapest price
        level.price = redis.call("ZRANGE", depthKey,0,0)[1]
        if (level.price ~= nil) then
            -- redis.call('GET',level.price)
            local levelPriceNumber = tonumber(level.price)
            local levelKey = depth.exchange .. ":" .. depth. product .. ":LEVELQTY:" .. depth.side .. ":" .. level.price
            level.qty = redis.call('GET',levelKey)
            local levelQtyNumber = tonumber(level.qty)
            if (verbose) then
                redis.call('PUBLISH','verboselog', 'FOUND A ' .. depth.side .. ' DEPTH LEVEL TO MATCH AGAINST: Qty=' .. level.qty .. ' Price=' .. level.price)
                redis.call('PUBLISH', 'debug', depthKey)
            end

            
            local qtyLevelReduction = 0
            while (leftOverQty > 0 and levelPriceNumber <= order.price and level.price ~= nil and overflow < 100000)
            do
                overflow = overflow + 1
                -- if the order has enough qty to consumer the whole level
                if (levelQtyNumber <= leftOverQty) then
                    -- full level fill
                    redis.call("ZPOPMIN", depthKey)
                    redis.call("DEL",levelKey)
                    depthUpdates = depthUpdates .. '{"qty":0, "price": ' .. level.price .. '} ,'
                    leftOverQty = leftOverQty - level.qty
                    qtyLevelReduction = level.qty
                        if (verbose) then
                            redis.call('PUBLISH','verboselog', 'CONSUMING ALL THAT DEPTH AND BROADCASTING IT SHOULD BE ZEROed OUT: ' .. leftOverQty .. ' is left to process in the order')
                        end
                else
                    -- otherwise simply reduce the qty of that level and zero out the leftover volume of the order as it'll be completely consumed by this level
                    newVol = redis.call("INCRBY",levelKey,leftOverQty *-1)
                    depthUpdates = depthUpdates .. '{"qty":' .. newVol .. ', "price": ' .. level.price .. '} ,'
                    qtyLevelReduction = leftOverQty
                    leftOverQty = 0
                    if (verbose) then
                        redis.call('PUBLISH','verboselog', 'CONSUMING SOME OF THAT DEPTH: Leaving it with a qty=' .. newVol .. ' AND NOW ORDER IS FULLY CONSUMED')
                    end
                end
                -- now we have to hunt for all the orders that composed this level and reduce their volume by order of time priority
                reduce_volume_matched_orders_at_this_level(level, qtyLevelReduction, order)

                -- now let's explore the next range
                level.price = redis.call("ZRANGE", depthKey,0,0)[1]
                if (level.price ~= nil) then
                    levelPriceNumber = tonumber(level.price)
                    levelKey = depth.exchange .. ":" .. depth.product .. ":LEVELQTY:"  .. depth.side .. ":" .. level.price
                    level.qty = redis.call('GET',levelKey)
                    levelQtyNumber = tonumber(level.qty)
                    if (verbose) then
                        redis.call('PUBLISH','verboselog', '2S- FOUND ANOTHER ' .. depth.side .. ' DEPTH LEVEL TO MATCH AGAIN: Qty=' .. level.qty .. ' Price=' .. level.price)
                    end
                end
            end -- end of exploring the sell depth
        end
        depthUpdates = depthUpdates:sub(1, -2) .. '], "buys": ['
        redis.call('SET', order.exchange .. ":ORDER:" .. order.number .. ":QTYLEFTOVER", leftOverQty)
        
        if (verbose) then
            redis.call('PUBLISH','verboselog', 'SETTING ORDER leftOverQty=' .. leftOverQty)
        end

        
        if (leftOverQty > 0) then -- if there is buy volume left after matching to sell depth, add to the buy depth
            if (verbose) then
                redis.call('PUBLISH','verboselog', 'Adding to depth ' .. leftOverQty)
            end
            add_volume_to_depth(order.exchange, order.product, order.side, order.price, leftOverQty, order.number, order.timestamp)
        end
        depthUpdates = depthUpdates .. "]}"
    else
                depthUpdates = depthUpdates .. '"buys": [  '
                -- now let's find the 1 level within that depth with the cheapest price
                level.price = redis.call("ZREVRANGE", depthKey,0,0)[1]
                if (level.price ~= nil) then
                    -- redis.call('GET',level.price)
                    local levelPriceNumber = tonumber(level.price)
                    local levelKey = depth.exchange .. ":" .. depth.product .. ":LEVELQTY:"  .. depth.side .. ":" .. level.price
                    level.qty = redis.call('GET',levelKey)
                    local levelQtyNumber = tonumber(level.qty)
                    if (verbose) then
                        redis.call('PUBLISH','verboselog', 'B- FOUND A ' .. depth.side .. ' DEPTH LEVEL TO MATCH AGAIN: Qty=' .. level.qty .. ' Price=' .. level.price .. ' DepthKey=' .. depthKey)
                        redis.call('PUBLISH', 'debug', depthKey)
                    end
            
                    local qtyLevelReduction = 0
                    while (leftOverQty > 0 and levelPriceNumber >= order.price and level.price ~= nil and overflow < 100000)
                    do
                        overflow = overflow + 1
                        -- if the order has enough qty to consumer the whole level
                        --redis.call('PUBLISH','debug','b: levelQtyNumber: ' .. levelQtyNumber .. " - leftOverQty: " .. leftOverQty)
                        if (levelQtyNumber <= leftOverQty) then
                            -- full level fill
                            redis.call("ZPOPMAX", depthKey) -- change correction
                            redis.call("DEL",levelKey)
                            depthUpdates = depthUpdates .. '{"qty":0, "price": ' .. level.price .. '} ,'
                            leftOverQty = leftOverQty - level.qty
                            qtyLevelReduction = level.qty
                            if (verbose) then
                                redis.call('PUBLISH','verboselog', 'CONSUMING ALL THAT DEPTH AND BROADCASTING IT SHOULD BE ZEROed OUT: ' .. leftOverQty .. ' is left to process in the order')
                            end
                        else
                            -- otherwise simply reduce the qty of that level and zero out the leftover volume of the order as it'll be completely consumed by this level
                            newVol = redis.call("INCRBY",levelKey,leftOverQty *-1)
                            depthUpdates = depthUpdates .. '{"qty":' .. newVol .. ', "price": ' .. level.price .. '} ,'
                            qtyLevelReduction = leftOverQty
                            leftOverQty = 0
                            if (verbose) then
                                redis.call('PUBLISH','verboselog', 'CONSUMING SOME OF THAT DEPTH: Leaving it with a qty=' .. newVol .. ' AND NOW ORDER IS FULLY CONSUMED')
                             end
                        end
                        -- now we have to hunt for all the orders that composed this level and reduce their volume by order of time priority
                        --redis.call('PUBLISH','debug',"A/Trying to reduce level of price " .. level.price .. " by a qty of " .. qtyLevelReduction .. " - Original order qty: " .. order.qty)
                        reduce_volume_matched_orders_at_this_level(level, qtyLevelReduction, order)
                        -- now let's explore the next range
                        level.price = redis.call("ZREVRANGE", depthKey,0,0)[1]
                        if (level.price ~= nil) then
                            levelPriceNumber = tonumber(level.price)
                            levelKey = depth.exchange .. ":" .. depth.product .. ":LEVELQTY:" .. depth.side .. ":" .. level.price
                            level.qty = redis.call('GET',levelKey)
                            levelQtyNumber = tonumber(level.qty)
                            if (verbose) then
                               redis.call('PUBLISH','verboselog', '2B- FOUND ANOTHER ' .. depth.side .. ' DEPTH LEVEL TO MATCH AGAIN: Qty=' .. level.qty .. ' Price=' .. level.price)
                            end
                        end
                    end -- end of exploring the sell depth
                end
                depthUpdates = depthUpdates:sub(1, -2) .. '], "sells": ['
                redis.call('SET', order.exchange .. ":ORDER:" .. order.number .. ":QTYLEFTOVER", leftOverQty)
                if (verbose) then
                   redis.call('PUBLISH','verboselog', 'SETTING ORDER leftOverQty=' .. leftOverQty)
                end
                if (leftOverQty > 0) then -- if there is buy volume left after matching to sell depth, add to the buy depth
                    add_volume_to_depth(order.exchange, order.product, order.side, order.price, leftOverQty, order.number, order.timestamp)
                end
                depthUpdates = depthUpdates .. "]}"
    end

    return order.number -- tostring(level.qty) .. ":" .. tostring(level.price)
end

local function createJson(order)
    return '{"exchange": "' .. order.exchange .. '","orderNumber": "' .. order.number .. '", "side": "' .. order.side .. '", "qty": ' .. order.qty .. ', "price": ' .. order.price .. ', "qtyLeft": ' .. redis.call('GET', order.exchange .. ':ORDER:' .. order.number .. ":QTYLEFTOVER") .. ', "fills": ['
end

local function updateBestPrice(exchange,product,price,side)
    local bestPrice = tonumber(redis.call("GET", exchange .. ":" .. product .. ":BESTPRICE"))
    local bestQty = 0
    -- let's check if we've already stored a best price and qty for this product
    if (bestPrice ~= nil) then
        bestQty =  tonumber(redis.call("GET", exchange .. ":" .. product .. ":BESTQTY"))
        -- now let's see if we are in the kind of conditions where we should update them
        if ((side == "S" and tonumber(price) <= bestPrice ) or (side == "B" and tonumber(price) >= bestPrice )) then
            -- impacted the best price/vol: either sold cheaper or at the same level OR bought higher or equal
            local bestPriceAndVol = redis.call("ZRANGE", exchange .. ":" .. product .. ":DEPTH:S" ,0,0) 
            if (bestPriceAndVol ~= nil) then
                for key,value in pairs(bestPriceAndVol) 
                do 
                    bestPrice = value
                end      
                bestQty = redis.call("GET", exchange .. ":" .. product .. ":LEVELQTY:S:" .. bestPrice)       
                redis.call("SET", exchange .. ":" .. product .. ":BESTPRICE", bestPrice)
                redis.call("SET", exchange .. ":" .. product .. ":BESTQTY", bestQty)
            end
        end
    else
        -- no previous set best price and qty so let's set it
        local bestPriceAndVol = redis.call("ZRANGE", exchange .. ":" .. product .. ":DEPTH:S" ,0,0)
        if (bestPriceAndVol ~= nil) then
            for key,value in pairs(bestPriceAndVol) 
            do 
                bestPrice = value
            end
            if (bestPrice ~= nil and bestQty ~= nil) then
                bestQty = redis.call("GET", exchange .. ":" .. product .. ":LEVELQTY:S:" .. bestPrice)       
                redis.call("SET", exchange .. ":" .. product .. ":BESTPRICE", bestPrice)
                redis.call("SET", exchange .. ":" .. product .. ":BESTQTY", bestQty)
            end
        end
    end
end


local order = {exchange = ARGV[1], product = ARGV[2], side = ARGV[3], qty = tonumber(ARGV[4]), price = tonumber(ARGV[5]), timestamp = tonumber(ARGV[6]), user = ARGV[7], number = 0}
if (verbose) then
    redis.call('PUBLISH','verboselog', '***** RECEIVED AN ORDER FROM LAMBDA: exchange=' .. order.exchange .. ' product=' .. order.product .. ' side=' .. order.side .. ' qty=' .. order.qty .. ' price=' .. order.price .. ' ts=' .. order.timestamp .. ' user=' .. order.user)
end
depthUpdates = '{"exchange":"' .. order.exchange .. '", "product":"' .. order.product .. '", '


redis.call('PUBLISH','debug','{exchange = ' .. ARGV[1] .. ', product = ' .. ARGV[2] .. ', side = ' .. ARGV[3] .. ', price = tonumber(' .. ARGV[5] .. '), qty = tonumber(' .. ARGV[4] .. '), timestamp = tonumber(' .. ARGV[6] .. '), user = ' .. ARGV[7] .. ', number = 0}')
local latency = redis.call('TIME')[2]
redis.call('PUBLISH','latency', 'in redis @ ' .. latency)
order.number = process_order(order)
if (verbose) then
    redis.call('PUBLISH','verboselog', 'attempting to set best price')
end
updateBestPrice(order.exchange,order.product,order.price,order.side)
local latency2 = redis.call('TIME')[2]
redis.call('PUBLISH','latency', 'out of redis @ ' .. latency2 .. " (" .. latency2 - latency .. " micros to process order)")

local orderReturn = createJson(order)

if (fillsCount >0) then
    orderReturn = orderReturn .. fillsJsonTable:sub(1, -2) -- remove the last comma
end
orderReturn = orderReturn .. ']}'

return {orderReturn,depthUpdates}
