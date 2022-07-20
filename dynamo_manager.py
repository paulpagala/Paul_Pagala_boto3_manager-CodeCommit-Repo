import logging
from decimal import *
import random
import json
import operator as op
import boto3
from boto3.dynamodb.conditions import Key, Attr



logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s %(module)s %(lineno)d - %(message)s',
)

log = logging.getLogger()

def create_dynamo_table(table_name, pk, pkdef):
    ddb = boto3.resource('dynamodb')
    table = ddb.create_table(
        TableName=table_name,
        KeySchema=pk,
        AttributeDefinitions=pkdef,
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5,
        }
    )

    table.meta.client.get_waiter('table_exists').wait(TableName=table_name)
    return table

#==================================================
#Use an existing table
def get_dynamo_table(table_name):
    ddb = boto3.resource('dynamodb')
    return ddb.Table(table_name)


#=================================================
#Create an item
def create_product(category, sku, **item):
    table = get_dynamo_table('products_pj')
    keys = {
        'category': category,
        'sku': sku,
    }
    item.update(keys)
    table.put_item(Item=item)
    return table.get_item(Key=keys)['Item']

#================================================
#Update an item
def update_product(category, sku, **item):
    table = get_dynamo_table('products_pj')
    keys = {
        'category': category,
        'sku': sku,
    }
    expr = ', '.join([f'{k}=:{k}' for k in item.keys()])
    vals = {f':{k}': v for k, v in item.items()}
    table.update_item(
        Key=keys,
        UpdateExpression=f'SET {expr}',
        ExpressionAttributeValues=vals,
    )
    return table.get_item(Key=keys)['Item']


#==============================================
#Delete an item
def delete_product(category, sku):
    table = get_dynamo_table('products_pj')
    keys = {
        'category': category,
        'sku': sku,
    }
    res = table.delete_item(Key=keys)
    if res.get('ResponseMetadata', {}).get('HTTPStatusCode') == 200:
        return True
    else:
        log.error(f'There was an error when deleting the product: {res}'
                )
        return False


#===========================================
#Batch Operatiions

def create_dynamo_items(table_name, items, keys=None):
    table = get_dynamo_table(table_name)
    params = {
        'overwrite_by_pkeys': keys
    } if keys else {}
    with table.batch_writer(**params) as batch:
        for item in items:
            batch.put_item(Item=item)
    return True


#============================================
#query_products
def query_products(key_expr, filter_expr=None):
    # Query requires that you provide the key filters
    table = get_dynamo_table('products_pj')
    params = {
            'KeyConditionExpression': key_expr,
            }
    if filter_expr:
        params['FilterExpression'] = filter_expr
    res = table.query(**params)
    return res['Items']


#==============================================
def scan_products(filter_expr):
    # Scan does not require a key filter. It will go through
    # all items in your table and return all matching items.
    # Use with caution!
    table = get_dynamo_table('products_pj')
    params = {
        'FilterExpression': filter_expr,
    }
    res = table.scan(**params)
    return res['Items']


#=================================================
def delete_dynamo_table(table_name):
    table = get_dynamo_table(table_name)
    table.delete()
    table.wait_until_not_exists()
    return True


if __name__=='__main__':
    #create table
    create_table = create_dynamo_table(
            'products_pj',
            pk=[
                {
                    'AttributeName': 'category',
                    'KeyType': 'HASH',
                },
                {
                    'AttributeName': 'sku',
                    'KeyType': 'RANGE',
                },
                ],
            pkdef=[
                {
                    'AttributeName': 'category',
                    'AttributeType': 'S',
                },
                {
                    'AttributeName': 'sku',
                    'AttributeType': 'S',
                },
                ],
            )
    log.info(f'{create_table}')
    #Get table
    table = get_dynamo_table('products_pj')
    log.info(f'{table.item_count}')


    #Create item
    product = create_product(
        'clothing', 'woo-hoodie927',
        product_name='Hoodie',
        is_published=True,
        price=Decimal('44.99'),
        in_stock=True
    )
    
    log.info(f'{product}')
    
    #update item
    product_update = update_product('clothing', 'woo-hoodie927', in_stock=False, price=Decimal('54.75'))
    log.info(f'{product_update}')

    #delete product
    product_delete = delete_product('clothing', 'woo-hoodie927')
    log.info(f'{product_delete}')
    

    #create batch files
    items = []
    sku_types = ('woo', 'foo')
    category = ('apparel', 'clothing', 'jackets')
    status = (True, False)
    prices = (Decimal('34.75'), Decimal('49.75'), Decimal('54.75'))
    for id in range(200):
        id += 1
        items.append({
            'category': random.choice(category),
            'sku': f'{random.choice(sku_types)}-hoodie-{id}',
            'product_name': f'Hoodie {id}',
            'is_published': random.choice(status),
            'price': random.choice(prices),
            'in_stock': random.choice(status),
        })
    #check if batch files were created
    create_batch_items = create_dynamo_items('products_pj', items, keys=['category','sku'])
    log.info(f'{create_batch_items}')
    
    #query specific item
    items = query_products(Key('category').eq('apparel') & Key('sku').begins_with('woo') )
    log.info(len(items))
    items = query_products(Key('category').eq('apparel') & Key('sku').begins_with('foo'))
    log.info(len(items))
    items = query_products(Key('category').eq('apparel') )
    log.info(len(items))
    items = query_products(Key('category').eq('apparel') & Key('sku').begins_with('foo'), filter_expr=Attr('in_stock').eq(True))
    log.info(len(items))

    #scan specific products
    items = scan_products(
        Attr('in_stock').eq(True)
    )
    log.info(len(items))
    items = scan_products(
        Attr('price').between(Decimal('30'), Decimal('40'))
    )
    log.info(len(items))
    items = scan_products(
        (
            Attr('in_stock').eq(True) & 
            Attr('price').between(Decimal('30'), Decimal('40'))
        )
    )
    log.info(len(items))

    
    #delete dynamo table
    delete_dynamo=delete_dynamo_table('products_pj')
    log.info(f'{delete_dynamo}')
    

