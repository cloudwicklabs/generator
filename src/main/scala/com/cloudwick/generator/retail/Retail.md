stores = 10
products = 10

date1 = "2015-07-01" (sunday)
date2 = "2015-07-08"

# If the date is sunday then the inventory is updated to 100 and the unit sale
# on that sunday would be any random number b/w 0-100.
# On monday the inventory is 100-previous days unit sale, and the unit sale is
# any random number b/w 0-()

| StoreID | ProductID | Date  | Inventory  | UnitSale |
| 1 | 1 | 2015-07-01 | 100 | 50 |
| 1 | 1 | 2015-07-02 | (100-previous_day_unitsale) = 50 | (random b/w 0-inventory_this_day) = 25 |
| 1 | 1 | 2015-07-03 | (previous_day_inventory - prevoius_day_unitsale) | (random b/w 0-inventory_this_day) |
| 1 | 1 | 2015-07-04 | (previous_day_inventory - prevoius_day_unitsale) | (random b/w 0-inventory_this_day) |
| 1 | 1 | 2015-07-05 | (previous_day_inventory - prevoius_day_unitsale) | (random b/w 0-inventory_this_day) |
| 1 | 1 | 2015-07-06 | (previous_day_inventory - prevoius_day_unitsale) | (random b/w 0-inventory_this_day) |
| 1 | 1 | 2015-07-07 | (previous_day_inventory - prevoius_day_unitsale) | (random b/w 0-inventory_this_day) |
| 1 | 1 | 2015-07-08 | (previous_day_inventory - prevoius_day_unitsale) | (random b/w 0-inventory_this_day) |

| 1 | 2 |
| 1 | 2 |
| 1 | 2 |
| 1 | 2 |
| 1 | 2 |
| 1 | 2 |
| 1 | 2 |
