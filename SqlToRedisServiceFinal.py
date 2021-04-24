# -*- coding: utf-8 -*-
from zato.server.service import Service
from contextlib import closing
import datetime
import json

class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime) or isinstance(obj, datetime.date):
            encoded_object = obj.strftime("%Y%m%d%H%M%S%z")
        else:
            encoded_object = json.JSONEncoder.default(self, obj)
        return encoded_object

class SqlToRedisServiceFinal(Service):
    name = 'Sql.To.Redis.Service.Final'

    class SimpleIO:
        input_required = ('query', 'out_name', 'output_dict_name', 'output_dict_key')
        input_optional = ('operation_name', 'table_name')

    def handle(self):
        query = self.request.input.query
        outName = self.request.input.out_name
        tableName = self.request.input.table_name
        outputDictName = self.request.input.output_dict_name
        outputDictKey = self.request.input.output_dict_key
        operationName = self.request.input.operation_name

        log_msg_head = """{} -- Stockage des donnees de la table '{}' dans Redis \n""".format(operationName, tableName)
        log_warn_head = """{} -- Erreur lors du stockage des donnees de la table '{}' dans Redis\n""".format(operationName, tableName)
        log_msg = log_warn = ""
        try:
            # Etape 1
            etape = "Etape 1"
            operation = """Selection dans la table '{}'""".format(tableName)
            inputVariable = {"query": query,
                             "out_name": outName,
                             "operation": "select"}
            response = self.invoke("Request.Sql.Service.Final", inputVariable) 
            result = response.get("response").get("result")
            log_msg += """{} -- {}: terminee \n""".format(etape, operation)
            # Etape 2
            etape = "Etape 2"
            operation = """Stockage dans Redis""".format(tableName)
            self.kvdb.conn.delete(outputDictName)
            for i in range(len(result)):
                row = json.dumps(result[i], cls=DateTimeEncoder)
                dictKey = result[i].get(outputDictKey)
                self.kvdb.conn.hset(outputDictName, dictKey, row)
            log_msg += """{} -- {}: termine \n""".format(etape, operation)
            self.logger.info(log_msg_head + log_msg)
        except Exception as es:
            log_warn += log_msg 
            log_warn += """{} -- {} -- Erreur : {} \n""".format(etape, operation, es)
            log_warn += """ ================== Erreur \n"""
            self.logger.warn(log_warn_head + log_warn)