package com.cloudwick.generator.retail

import org.joda.time.LocalDate

case class RetailEvent(storeId: Int, productId: Int, date: LocalDate, inventory: Int, unitSale: Int) {
    override def toString = s"$storeId $productId $date $inventory $unitSale\n"
}
