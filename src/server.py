import asyncio
import sanic
import json
import os.path
import time
from sanic.log import logger
from connection_manager import ConnectionManager

log_params = sanic.log.LOGGING_CONFIG_DEFAULTS
fname = time.asctime().replace(' ','_').replace(':','') + '.log'
for h in log_params['handlers']:
    if 'stream' in log_params['handlers'][h]:
        del log_params['handlers'][h]['stream']
        log_params['handlers'][h]['class'] = 'logging.FileHandler'
    log_params['handlers'][h]['filename'] = os.path.join('..', 
                                                         'server_logs', 
                                                         fname)

CM = ConnectionManager()
kv_server = sanic.Sanic(log_config=log_params)

async def check_exist(kid):
    conn = await CM.get_connection()
    retval = (1, sanic.response.HTTPResponse(status=404))

    try:
        logger.info(f'Searching for pair, key: "{kid}"')
        res = await conn.select('kv', key=[kid], index='primary')
        if len(res) != 0:
            logger.info(f'Found pair, key: "{kid}", value: "{res[0]["value"]}"')
            retval = (0, sanic.response.json(res[0]['value']))
        else:
            logger.error(f'Error on searching for pair, key: "{kid}", ' +\
                         f'error: pair not found')
    except BaseException as be:
        errmsg = f'Exception was thrown while searching for kv pair, ' +\
                 f'key: "{kid}", details: "{be}"'
        logger.exception(errmsg)
        retval = (2, sanic.response.HTTPResponse(status=500))
    finally:
        CM.close_connection(conn)

    return retval

async def update_value(kid, data_str):
    conn = await CM.get_connection()
    retval = (2, sanic.response.HTTPResponse(status=500))

    try:
        logger.info(f'Updating pair, key: "{kid}", body: {data_str}')
        body_err = False
        try:
            data = json.loads(data_str)
            if len(data) != 1 or \
               'value' not in data or \
               type(data['value']) != type(dict()):
                retval = (2, sanic.response.HTTPResponse(status=400))
                body_err = True

            if not body_err:
                data_str = json.dumps(data['value'])
        except JSONDecodeError as _:
            retval = (2, sanic.response.HTTPResponse(status=400))
            body_err = True

        if not body_err:
            res = await conn.update('kv', [kid], [['=', 'value', data_str]])
            logger.info(f'Updated pair, key: "{kid}", value: "{data_str}"')
            retval = (0, sanic.response.HTTPResponse(status=200))
        else:
            logger.error(f'Error on updating pair, ' +\
                         f'key: "{kid}", error: bad json body')
            retval = (2, sanic.response.HTTPResponse(status=400))
    except BaseException as be:
        errmsg = f'Exception was thrown while updating kv pair, key: "{kid}", ' +\
                 f'body: "{data_str}", details: "{be}"'
        logger.exception(errmsg)
        retval = (2, sanic.response.HTTPResponse(status=500))
    finally:
        CM.close_connection(conn)

    return retval

async def add_kv(data_str):
    conn = await CM.get_connection()
    retval = (2, sanic.response.HTTPResponse(status=500))

    try:
        logger.info(f'Adding new pair, body: "{data_str}"')
        body_err = False
        try:
            data = json.loads(data_str)
            if len(data) != 2 or \
               'value' not in data or \
               'key' not in data or \
               type(data['value']) != type(dict()):
                retval = (2, sanic.response.HTTPResponse(status=400))
                body_err = True
        except JSONDecodeError:
            retval = (2, sanic.response.HTTPResponse(status=400))
            body_err = True

        if not body_err:
            retc, retval = await check_exist(str(data['key']))

            if retc == 0:
                logger.error(f'Error on adding new pair, ' +\
                             f'body: {data_str}, error: key already exists')
                retval = (2, sanic.response.HTTPResponse(status=409))
            elif retc == 1:
                res = await conn.insert('kv', [str(data['key']), str(data['value'])])
                logger.info(f'Added new pair, key: "{data["key"]}", ' +\
                            f'value: "{data["value"]}"')
                retval = (0, sanic.response.HTTPResponse(status=200))
        else:
            logger.error(f'Error on addding new pair, ' +\
                         f'body: "{data_str}", error: bad json body')
            retval = (2, sanic.response.HTTPResponse(status=400))
    except BaseException as be:
        errmsg = f'Exception was thrown while adding new kv pair, ' +\
                 f'body: "{data_str}", details: "{be}"'
        logger.exception(errmsg)
        retval = (2, sanic.response.HTTPResponse(status=500))
    finally:
        CM.close_connection(conn)

    return retval

async def delete_key(kid):
    conn = await CM.get_connection()
    retval = (2, sanic.response.HTTPResponse(status=500))

    try:
        logger.info(f'Deleting key "{kid}"')
        res = await conn.delete('kv', [kid])
        logger.info(f'Deleted key "{kid}", tarantool\'s response: "{res}"')
        retval = (0, sanic.response.HTTPResponse(status=200))
    except BaseException as be:
        errmsg = f'Exception was thrown while deleting kv pair, key: "{kid}", ' +\
                 f'details: "{be}"'
        logger.exception(errmsg)
        retval = (2, sanic.response.HTTPResponse(status=500))
    finally:
        CM.close_connection(conn)

    return retval
    

@kv_server.route('/kv/<kid:string>', methods=['GET', 'PUT', 'DELETE'])
async def handle_with_id(req, kid):
    retval = None

    try:
        retc, retval = await check_exist(kid)
        if retc == 0:
            if req.method == 'GET':
                logger.info(f'{req.method}')
            elif req.method == 'PUT':
                bs = str(req.body, encoding='UTF-8')
                logger.info(f'{req.method} with body "{bs}"')
                retc, retval = await update_value(kid, bs)
            elif req.method == 'DELETE':
                logger.info(f'{req.method}')
                retc, retval = await delete_key(kid)
    except BaseException as be:
        errmsg = f'Exception was thrown while processing response, details: {be}'
        logger.exception(errmsg)
        retval = sanic.response.HTTPResponse(status=500)
    
    return retval

@kv_server.route('/kv', methods=['POST'])
async def handle_new_kv(req):
    retval = None

    try:
        bs = str(req.body, encoding='UTF-8')
        logger.info(f'{req.method} with body "{bs}"')
        retc, retval = await add_kv(bs)
    except BaseException as be:
        errmsg = f'Exception was thrown while processing response, details: {be}'
        logger.exception(errmsg)
        retval = sanic.response.HTTPResponse(status=500)
    
    return retval

@kv_server.listener('before_server_start')
async def run_cm(_, __):
    global CM

    await CM.init()

@kv_server.listener('after_server_start')
async def run_cm(_, __):
    print('Server started')

@kv_server.listener('before_server_stop')
async def kill_cm(_, __):
    global CM

    await CM.close_all()

@kv_server.listener('after_server_stop')
async def run_cm(_, __):
    print('Server stopped')
    
if __name__ == '__main__':
    kv_server.run(host='0.0.0.0', \
                  port=80, \
                  workers=4, \
                  log_config=log_params)
        
