/*
 *  AMQPMessage.cpp
 *  librabbitmq++
 *
 *  Created by Alexandre Kalendarev on 15.04.10.
 *
 */

#include "AMQPcpp.h"

using namespace std;

AMQPMessage::AMQPMessage( AMQPQueue * queue ) {
	this->queue=queue;
	 message_count=-1;
	 data=NULL;
}

AMQPMessage::~AMQPMessage() {
	if (data) {
		free(data);
	}
}

void AMQPMessage::setMessage(const char * data,size_t length) {
	if (!data)
		return;

	if (this->data)
		free(this->data);

	this->data = (char*)malloc(length + 1);
	if (!this->data) {
		throw AMQPException("setMessage: malloc failed");
	}
	memcpy(this->data,data,length);
	this->data[length] = '\0';
	this->len = length;
}

char * AMQPMessage::getMessage(size_t* length) {
	if (this->data)
	  {
	    *length = this->len;
	    return this->data;
	  }
	*length = 0;
	return (char*) '\0';
}

string AMQPMessage::getConsumerTag() {
	return this->consumer_tag;
}

void AMQPMessage::setConsumerTag(amqp_bytes_t consumer_tag) {
	this->consumer_tag.assign( (char*)consumer_tag.bytes, consumer_tag.len );
}

void AMQPMessage::setConsumerTag(string consumer_tag) {
	this->consumer_tag=consumer_tag;
}

void AMQPMessage::setDeliveryTag(uint64_t delivery_tag) {
	this->delivery_tag=delivery_tag;
}

uint64_t AMQPMessage::getDeliveryTag() {
	return this->delivery_tag;
}

void AMQPMessage::setMessageCount(int count) {
	this->message_count = count;
}

int AMQPMessage::getMessageCount() {
	return message_count;
}

void AMQPMessage::setExchange(amqp_bytes_t exchange) {
	if (exchange.len)
		this->exchange.assign( (char*)exchange.bytes, exchange.len );
}

void AMQPMessage::setExchange(string exchange) {
	this->exchange = exchange;
}

string AMQPMessage::getExchange() {
	return exchange;
}

void AMQPMessage::setRoutingKey(amqp_bytes_t routing_key) {
	if (routing_key.len)
		this->routing_key.assign( (char*)routing_key.bytes, routing_key.len );
}

void AMQPMessage::setRoutingKey(string routing_key) {
	this->routing_key=routing_key;
}

string AMQPMessage::getRoutingKey() {
	return routing_key;
}

void AMQPMessage::addHeader(string name, amqp_bytes_t * value) {
	string svalue;
	svalue.assign(( const char *) value->bytes, value->len);
	headers[name] = svalue;
	//headers.insert( pair<string,string>(name,svalue));
}

void AMQPMessage::addHeader(string name, uint64_t * value) {
	headers[name] = std::to_string(*value);
	//headers.insert(pair<string,string>(name,string(ivalue)));
}

void AMQPMessage::addHeader(string name, uint8_t * value) {
	headers[name] = std::to_string(*value);
	//headers.insert( pair<string,string>(name,string(ivalue)));
}

void AMQPMessage::addHeader(amqp_bytes_t * name, amqp_bytes_t * value) {
	//cout << "name " << name << endl;
	string sname;
	sname.assign((const char *) name->bytes, name->len);
	string svalue;
	svalue.assign((const char *) value->bytes, value->len);
	headers[sname] = string(svalue);
	//headers.insert(pair<string, string>(sname, svalue));
}

void AMQPMessage::addHeader(const amqp_table_entry_t* entry)
{
	string sname;
	sname.assign((const char*)entry->key.bytes, entry->key.len);
	headers[sname] = [](const amqp_field_value_t* value) {
		switch (value->kind) {
		case AMQP_FIELD_KIND_BOOLEAN: return std::to_string(value->value.boolean);
		case AMQP_FIELD_KIND_I8:  return std::to_string(value->value.i8);
		case AMQP_FIELD_KIND_U8:  return std::to_string(value->value.u8);
		case AMQP_FIELD_KIND_I16: return std::to_string(value->value.i16);
		case AMQP_FIELD_KIND_U16: return std::to_string(value->value.u16);
		case AMQP_FIELD_KIND_I32: return std::to_string(value->value.i32);
		case AMQP_FIELD_KIND_U32: return std::to_string(value->value.u32);
		case AMQP_FIELD_KIND_I64: return std::to_string(value->value.i64);
		case AMQP_FIELD_KIND_U64: return std::to_string(value->value.u64);
		case AMQP_FIELD_KIND_UTF8: {
			string svalue;
			svalue.assign((const char*) value->value.bytes.bytes, value->value.bytes.len);
			return svalue;
		}
		default:
			throw std::runtime_error("Unexpected kind: " + std::to_string(value->kind));
		}
	}(&entry->value);
}

string AMQPMessage::getHeader(string name) const {
	const auto i = headers.find(name);
	if (i == headers.end())
		return "";
	else
		return i->second;
}

AMQPQueue * AMQPMessage::getQueue() {
	return queue;
}
