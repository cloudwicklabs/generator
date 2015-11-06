package com.cloudwick.generator.manufacturer

case class ManufacturerEvent(
  manufacturerName: String,
  manufacturerPartNumber: String,
  category: String,
  entityType: String, // "PART"
  entityKey: String, // manuf_name|random
  recordType: String, // MASTER or DOCUMENT
  recordTypeKey: String,
  genericNumber: String
)