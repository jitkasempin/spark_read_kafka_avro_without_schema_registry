/*
 *
 * Copyright 2018 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


// This file dong edit

package abris.avro.format

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.sql.{Date, Timestamp}
import java.util.HashMap

import com.databricks.spark.avro.SchemaConverters
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.{GenericRecord, IndexedRecord}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import abris.avro.read.confluent.ConfluentConstants
import abris.avro.write.AvroWriterHolder

import scala.collection._


/**
 * This class provides conversions between Avro and Spark schemas and data.
 */
object SparkAvroConversions {

  private case class ConverterKey(sparkSchema: DataType, recordName: String, recordNamespace: String) 
  private val converterCache = new mutable.HashMap[ConverterKey, Any => Any]()
  
  private def avroWriterHolder = new AvroWriterHolder()

  /**
    * Attaches the id to the beginning of an output stream.
    */
  private def attachSchemaId(id: Int, outStream: ByteArrayOutputStream) = {
    outStream.write(ConfluentConstants.MAGIC_BYTE)
    outStream.write(ByteBuffer.allocate(ConfluentConstants.SCHEMA_ID_SIZE_BYTES).putInt(id).array())
  }

  /**
   * Converts an Avro record into an Array[Byte].
   * 
   * Uses Avro's Java API under the hood.
   */
  def toByteArray(record: IndexedRecord, schema: Schema, schemaId: Option[Int] = None): Array[Byte] = {
    val outStream = new ByteArrayOutputStream()

    if (schemaId.isDefined) {
      attachSchemaId(schemaId.get, outStream)
    }

    val encoder = avroWriterHolder.getEncoder(outStream)
    try {                 
      avroWriterHolder.getWriter(schema).write(record, encoder)
      encoder.flush()
      outStream.flush()
      outStream.toByteArray()
    } finally {      
      outStream.close()
    }    
  }  
  
  /**
   * Converts a Spark's SQL type into an Avro schema, using specific names and namespaces for the schema.
   */
  def toAvroSchema(
      structType: StructType,
      schemaName: String,
      schemaNamespace: String): Schema = {
    val build = SchemaBuilder.record(schemaName).namespace(schemaNamespace)    
//    DatabricksAdapter.convertStructToAvro(structType, build, schemaNamespace)

    SchemaConverters.convertStructToAvro(structType, build, schemaNamespace)

  }
  
  /**
   * Converts a Spark Row into an Avro's binary record.
   */
  def rowToBinaryAvro(row: Row, sparkSchema: StructType, avroSchema: Schema, schemaId: Option[Int] = None) = {
    val record = rowToGenericRecord(row, sparkSchema, avroSchema)    
    toByteArray(record, avroSchema, schemaId)
  }

  /**
   * Translates an Avro Schema into a Spark's StructType.
   * 
   * Relies on Databricks Spark-Avro library to do the job.
   */
  def toSqlType(schema: Schema): StructType = {
//    DatabricksAdapter.toSqlType(schema).dataType.asInstanceOf[StructType]

    SchemaConverters.toSqlType(schema).dataType.asInstanceOf[StructType]

  }
  
  private def rowToGenericRecord(row: Row, sparkSchema: StructType, avroSchema: Schema) = {
    val converter = getConverter(sparkSchema, avroSchema.getName, avroSchema.getNamespace)
    converter(row).asInstanceOf[GenericRecord]
  }
  
  private def getConverter(dataType: DataType, name: String, namespace: String): Any => Any = {
    converterCache.getOrElseUpdate(new ConverterKey(dataType, name, namespace), SparkAvroConversions.createConverterToAvro(dataType, name, namespace)) 
  }


  private[avro] def getTHeNewRecordNamespace(
                                           elementDataType: DataType,
                                           currentRecordNamespace: String,
                                           elementName: String): String = {

    elementDataType match {
      case StructType(_) => s"$currentRecordNamespace.$elementName"
      case _ => currentRecordNamespace
    }
  }



  // Copied from Databricks, as any implementation would be very similar and this library already uses Databricks'.
  // This method is private inside "com.databricks.spark.avro.AvroOutputWriter".
  private def createConverterToAvro(
    dataType:        DataType,
    structName:      String,
    recordNamespace: String): (Any) => Any = {
    dataType match {
      case BinaryType => (item: Any) => item match {
        case null               => null
        case bytes: Array[Byte] => ByteBuffer.wrap(bytes)
      }
      case ByteType | ShortType | IntegerType | LongType |
        FloatType | DoubleType | StringType | BooleanType => identity
      case _: DecimalType => (item: Any) => if (item == null) null else item.toString
      case TimestampType => (item: Any) =>
        if (item == null) null else item.asInstanceOf[Timestamp].getTime
      case DateType => (item: Any) =>
        if (item == null) null else item.asInstanceOf[Date].getTime
      case ArrayType(elementType, _) =>
        val elementConverter = createConverterToAvro(
          elementType,
          structName,
//          DatabricksAdapter.getNewRecordNamespace(elementType, recordNamespace, structName))
          SparkAvroConversions.getTHeNewRecordNamespace(elementType, recordNamespace, structName))

          (item: Any) => {
          if (item == null) {
            null
          } else {
            val sourceArray = item.asInstanceOf[Seq[Any]]
            val sourceArraySize = sourceArray.size
            val targetArray = new Array[Any](sourceArraySize)
            var idx = 0
            while (idx < sourceArraySize) {
              targetArray(idx) = elementConverter(sourceArray(idx))
              idx += 1
            }
            targetArray
          }
        }
      case MapType(StringType, valueType, _) =>
        val valueConverter = createConverterToAvro(
          valueType,
          structName,
//          DatabricksAdapter.getNewRecordNamespace(valueType, recordNamespace, structName))
          SparkAvroConversions.getTHeNewRecordNamespace(valueType, recordNamespace, structName))
        (item: Any) => {
          if (item == null) {
            null
          } else {
            val javaMap = new HashMap[String, Any]()
            item.asInstanceOf[Map[String, Any]].foreach {
              case (key, value) =>
                javaMap.put(key, valueConverter(value))
            }
            javaMap
          }
        }
      case structType: StructType =>
        val builder = SchemaBuilder.record(structName).namespace(recordNamespace)
        val schema: Schema = SchemaConverters.convertStructToAvro(
          structType, builder, recordNamespace)
        val fieldConverters = structType.fields.map(field =>
          createConverterToAvro(
            field.dataType,
            field.name,
//            DatabricksAdapter.getNewRecordNamespace(field.dataType, recordNamespace, field.name)))
            SparkAvroConversions.getTHeNewRecordNamespace(field.dataType, recordNamespace, field.name)))
        (item: Any) => {
          if (item == null) {
            null
          } else {
            val record = new Record(schema)            
            val convertersIterator = fieldConverters.iterator
            val fieldNamesIterator = dataType.asInstanceOf[StructType].fieldNames.iterator
            val rowIterator = item.asInstanceOf[Row].toSeq.iterator

            while (convertersIterator.hasNext) {
              val converter = convertersIterator.next()
              record.put(fieldNamesIterator.next(), converter(rowIterator.next()))
            }
            record
          }
        }
    }
  }  
}