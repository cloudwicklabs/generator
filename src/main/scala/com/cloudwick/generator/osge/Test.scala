package com.cloudwick.generator.osge

/**
 * Description goes here
 * @author ashrith 
 */
object Test extends App {
  var person: Person = null
  var address: Address = null
  var customer: Customers = null
  val counter = collection.mutable.Map(
    "male" -> 0,
    "female" -> 0
  )
  1 to 10 foreach { cID =>
//    person = new Person
//    address = new Address
//    if (person.gender == "male") {
//      counter("male") += 1
//    } else {
//      counter("female") += 1
//    }
//    println("Gender: " + person.gender + " Name: " + person.name + " Age: " + person.age)
//    customer = new Customers(cID, person.name, person.gender)
//    println(customer)
    println(new OSGEGenerator().eventGenerate)
  }
//  println(counter)
}
