/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package com.cloudwick.generator.avro;  
@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class Revenue extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Revenue\",\"namespace\":\"com.cloudwick.generator.avro\",\"fields\":[{\"name\":\"CId\",\"type\":\"string\"},{\"name\":\"CPaidDate\",\"type\":\"long\"},{\"name\":\"CRevenue\",\"type\":\"int\"}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
  @Deprecated public java.lang.CharSequence CId;
  @Deprecated public long CPaidDate;
  @Deprecated public int CRevenue;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>. 
   */
  public Revenue() {}

  /**
   * All-args constructor.
   */
  public Revenue(java.lang.CharSequence CId, java.lang.Long CPaidDate, java.lang.Integer CRevenue) {
    this.CId = CId;
    this.CPaidDate = CPaidDate;
    this.CRevenue = CRevenue;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return CId;
    case 1: return CPaidDate;
    case 2: return CRevenue;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: CId = (java.lang.CharSequence)value$; break;
    case 1: CPaidDate = (java.lang.Long)value$; break;
    case 2: CRevenue = (java.lang.Integer)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'CId' field.
   */
  public java.lang.CharSequence getCId() {
    return CId;
  }

  /**
   * Sets the value of the 'CId' field.
   * @param value the value to set.
   */
  public void setCId(java.lang.CharSequence value) {
    this.CId = value;
  }

  /**
   * Gets the value of the 'CPaidDate' field.
   */
  public java.lang.Long getCPaidDate() {
    return CPaidDate;
  }

  /**
   * Sets the value of the 'CPaidDate' field.
   * @param value the value to set.
   */
  public void setCPaidDate(java.lang.Long value) {
    this.CPaidDate = value;
  }

  /**
   * Gets the value of the 'CRevenue' field.
   */
  public java.lang.Integer getCRevenue() {
    return CRevenue;
  }

  /**
   * Sets the value of the 'CRevenue' field.
   * @param value the value to set.
   */
  public void setCRevenue(java.lang.Integer value) {
    this.CRevenue = value;
  }

  /** Creates a new Revenue RecordBuilder */
  public static com.cloudwick.generator.avro.Revenue.Builder newBuilder() {
    return new com.cloudwick.generator.avro.Revenue.Builder();
  }
  
  /** Creates a new Revenue RecordBuilder by copying an existing Builder */
  public static com.cloudwick.generator.avro.Revenue.Builder newBuilder(com.cloudwick.generator.avro.Revenue.Builder other) {
    return new com.cloudwick.generator.avro.Revenue.Builder(other);
  }
  
  /** Creates a new Revenue RecordBuilder by copying an existing Revenue instance */
  public static com.cloudwick.generator.avro.Revenue.Builder newBuilder(com.cloudwick.generator.avro.Revenue other) {
    return new com.cloudwick.generator.avro.Revenue.Builder(other);
  }
  
  /**
   * RecordBuilder for Revenue instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<Revenue>
    implements org.apache.avro.data.RecordBuilder<Revenue> {

    private java.lang.CharSequence CId;
    private long CPaidDate;
    private int CRevenue;

    /** Creates a new Builder */
    private Builder() {
      super(com.cloudwick.generator.avro.Revenue.SCHEMA$);
    }
    
    /** Creates a Builder by copying an existing Builder */
    private Builder(com.cloudwick.generator.avro.Revenue.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.CId)) {
        this.CId = data().deepCopy(fields()[0].schema(), other.CId);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.CPaidDate)) {
        this.CPaidDate = data().deepCopy(fields()[1].schema(), other.CPaidDate);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.CRevenue)) {
        this.CRevenue = data().deepCopy(fields()[2].schema(), other.CRevenue);
        fieldSetFlags()[2] = true;
      }
    }
    
    /** Creates a Builder by copying an existing Revenue instance */
    private Builder(com.cloudwick.generator.avro.Revenue other) {
            super(com.cloudwick.generator.avro.Revenue.SCHEMA$);
      if (isValidValue(fields()[0], other.CId)) {
        this.CId = data().deepCopy(fields()[0].schema(), other.CId);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.CPaidDate)) {
        this.CPaidDate = data().deepCopy(fields()[1].schema(), other.CPaidDate);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.CRevenue)) {
        this.CRevenue = data().deepCopy(fields()[2].schema(), other.CRevenue);
        fieldSetFlags()[2] = true;
      }
    }

    /** Gets the value of the 'CId' field */
    public java.lang.CharSequence getCId() {
      return CId;
    }
    
    /** Sets the value of the 'CId' field */
    public com.cloudwick.generator.avro.Revenue.Builder setCId(java.lang.CharSequence value) {
      validate(fields()[0], value);
      this.CId = value;
      fieldSetFlags()[0] = true;
      return this; 
    }
    
    /** Checks whether the 'CId' field has been set */
    public boolean hasCId() {
      return fieldSetFlags()[0];
    }
    
    /** Clears the value of the 'CId' field */
    public com.cloudwick.generator.avro.Revenue.Builder clearCId() {
      CId = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /** Gets the value of the 'CPaidDate' field */
    public java.lang.Long getCPaidDate() {
      return CPaidDate;
    }
    
    /** Sets the value of the 'CPaidDate' field */
    public com.cloudwick.generator.avro.Revenue.Builder setCPaidDate(long value) {
      validate(fields()[1], value);
      this.CPaidDate = value;
      fieldSetFlags()[1] = true;
      return this; 
    }
    
    /** Checks whether the 'CPaidDate' field has been set */
    public boolean hasCPaidDate() {
      return fieldSetFlags()[1];
    }
    
    /** Clears the value of the 'CPaidDate' field */
    public com.cloudwick.generator.avro.Revenue.Builder clearCPaidDate() {
      fieldSetFlags()[1] = false;
      return this;
    }

    /** Gets the value of the 'CRevenue' field */
    public java.lang.Integer getCRevenue() {
      return CRevenue;
    }
    
    /** Sets the value of the 'CRevenue' field */
    public com.cloudwick.generator.avro.Revenue.Builder setCRevenue(int value) {
      validate(fields()[2], value);
      this.CRevenue = value;
      fieldSetFlags()[2] = true;
      return this; 
    }
    
    /** Checks whether the 'CRevenue' field has been set */
    public boolean hasCRevenue() {
      return fieldSetFlags()[2];
    }
    
    /** Clears the value of the 'CRevenue' field */
    public com.cloudwick.generator.avro.Revenue.Builder clearCRevenue() {
      fieldSetFlags()[2] = false;
      return this;
    }

    @Override
    public Revenue build() {
      try {
        Revenue record = new Revenue();
        record.CId = fieldSetFlags()[0] ? this.CId : (java.lang.CharSequence) defaultValue(fields()[0]);
        record.CPaidDate = fieldSetFlags()[1] ? this.CPaidDate : (java.lang.Long) defaultValue(fields()[1]);
        record.CRevenue = fieldSetFlags()[2] ? this.CRevenue : (java.lang.Integer) defaultValue(fields()[2]);
        return record;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }
}
