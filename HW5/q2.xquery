for $part in distinct-values(doc("purchaseorders.xml")//item/partid/text())
let $price := doc("products.xml")//product[@pid = $part]//price/text()
let $items := doc("purchaseorders.xml")//item[partid = $part]
    order by $part
return
    <totalcost
        partid="{$part}">{sum($items/quantity) * $price}</totalcost>