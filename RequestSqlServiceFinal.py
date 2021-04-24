from zato.server.service import Service
from contextlib import closing


class RequestSQLServiceFinal(Service):
    name = 'Request.Sql.Service.Final'

    class SimpleIO:
        input_required = ('query', 'out_name', 'operation')
        input_optional = ('table_name', 'step')
        output_optional = ('result', 'status_code', 'status_reason')

    def handle(self):
        ip_source = self.wsgi_environ.get("HTTP_X_FORWARDED_FOR", None)
        self.logger.info(
            "service_name : requestSQLService,\
                req_id : {}, ip_source {} .".format(
                self.cid, ip_source
            )
        )
        self.logger.info(self.wsgi_environ)
        result = []
        status_reason = status_code = ''
        query = self.request.input.query
        out_name = self.request.input.out_name
        operation = self.request.input.operation
        table_name = self.request.input.table_name
        step = self.request.input.step

        try:
            if operation.lower() == "select":
                with closing(
                    self.outgoing.sql.get(out_name).session()
                ) as session:
                    resultQuery = session.execute(query)
                    result = [{
                        key: value for key, value in row.items()
                    } for row in resultQuery]
                    status_code = '200'
                    status_reason = 'operation success'

            elif (
                operation.lower() == "insert"
                or operation.lower() == "update"
                or operation.lower() == "delete"
            ):
                with closing(
                    self.outgoing.sql.get(out_name).session()
                ) as session:
                    session.execute(query)
                    session.commit()
                    self.logger.info("\n\n======== Request {} to {} finish - step : {}".format(
                        operation, table_name, step))
                    status_code = '200'
                    status_reason = 'operation success'
            else:
                status_reason = 'bad operation'
                status_code = '400'
        except Exception as e:
            status_code = '500'
            status_reason = "Connexion Error: {}".format(e)
            self.logger.info("\n\n======== Request Error {} to {} finish - step : {} - {}".format(
                operation, table_name, step, status_reason))

        response = {
            'result': result,
            'status_code': status_code,
            'status_reason': status_reason
        }
        self.response.payload = response
        self.log_output(self.name)