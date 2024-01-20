import pika, json, os
from sqlalchemy.exc import IntegrityError

from main import Product, db

params = pika.URLParameters(os.environ['RABBITMQ_URL'])

connection = pika.BlockingConnection(params)

channel = connection.channel()

channel.queue_declare(queue='main')


def callback(ch, method, properties, body):
    print('Received in main')
    data = json.loads(body)
    print(data)

    if properties.content_type == 'product_created':
        try:
            product = Product(id=data['id'], title=data['title'], image=data['image'])
            db.session.add(product)
            db.session.commit()
        except IntegrityError:
            db.session.rollback()

    elif properties.content_type == 'product_updated':
        product = Product.query.get(data['id'])
        if product is not None:
            product.title = data['title']
            product.image = data['image']
            db.session.commit()

    elif properties.content_type == 'product_deleted':
        product = Product.query.get(data)
        if product is not None:
            db.session.delete(product)
            db.session.commit()


channel.basic_consume(queue='main', on_message_callback=callback, auto_ack=True)

print('Started Consuming')

channel.start_consuming()

channel.close()
