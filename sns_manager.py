import boto3
import logging 


logging.basicConfig(
            level=logging.INFO,
                format='[%(asctime)s] %(levelname)s %(module)s %(lineno)d - %(message)s',
                )
log = logging.getLogger()

def create_sns_topic(topic_name):
    sns = boto3.client('sns')
    sns.create_topic(Name=topic_name)
    return True


def list_sns_topics(next_token=None):
    sns = boto3.client('sns')
    params = {'NextToken': next_token} if next_token else {}
    topics = sns.list_topics(**params)
    return topics.get('Topics', []), topics.get('NextToken', None)

def list_sns_subscriptions(next_token=None):
    sns = boto3.client('sns')
    params = {'NextToken': next_token} if next_token else {}
    subscriptions = sns.list_subscriptions(**params)
    return subscriptions.get('Subscriptions', []),subscriptions.get('NextToken', None)


def subscribe_sns_topic(topic_arn, mobile_number):
    sns = boto3.client('sns')
    params = {
        'TopicArn': topic_arn,
        'Protocol': 'sms',
        'Endpoint': mobile_number,
        }
    res = sns.subscribe(**params)
    print(res)
    return True

def send_sns_message(topic_arn, message):
    sns = boto3.client('sns')
    params = {
        'TopicArn': topic_arn,
        'Message': message,
            }
    res = sns.publish(**params)
    print(res)
    return True

def unsubscribe_sns_topic(subscription_arn):
    sns = boto3.client('sns')
    params = {
            'SubscriptionArn': subscription_arn,
            }
    res = sns.unsubscribe(**params)
    print(res)
    return True

def delete_sns_topic(topic_arn):
    # This will delete the topic and all it's subscriptions.
    sns = boto3.client('sns')
    sns.delete_topic(TopicArn=topic_arn)
    return True

if __name__ == '__main__':

    create_topic = create_sns_topic('price_updates_pj')
    log.info(f'{create_topic}')
    
    list_topics = list_sns_topics()
    log.info(f'{list_topics}')
    
    subscribe_topic = subscribe_sns_topic('arn:aws:sns:ap-southeast-1:337008671328:price_updates_pj', '+639053036103')
    log.info(f'{subscribe_topic}')

    list_subscriptions = list_sns_subscriptions()
    log.info(f'{list_subscriptions}')
    
    send_message = send_sns_message('arn:aws:sns:ap-southeast-1:337008671328:price_updates_pj', 'Woo Hoodies are no 50% off!')
    log.info(f'{send_message}')

    list_subscriptions = list_sns_subscriptions()
    log.info(f'{list_subscriptions}')
    
    unsubscribe_topics = unsubscribe_sns_topic('arn:aws:sns:ap-southeast-1:337008671328:price_updates_pj:03373dbd-443c-4a52-b73e-ccc0cd301a5b')
    log.info(f'{unsubcribe_topics}')

    list_topics = list_sns_topics()
    log.info(f'{list_topics}')
    
    
    delete_topic = delete_sns_topic('arn:aws:sns:ap-southeast-1:337008671328:price_updates_pj')
    log.info('f{delete_topic}')

    list_topics = list_sns_topics()
    log.info(f'{list_topics}')
    
    

