const { Kafka } = require('kafkajs')

const kafka = new Kafka({
  clientId: 'rocket_app',
  brokers: ['127.0.0.1:9092']
})

const consumer = kafka.consumer({ groupId: 'pagto' })
const producer = kafka.producer()


const run = async () => {

  await consumer.connect()
  await consumer.subscribe({ topic: 'ORDEM_RECEBIDA', fromBeginning: true })
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      var pedido = JSON.parse(message.value.toString());
      await processarPagamento(pedido);
    },
  })
}
const processarPagamento = async (pedido) =>{
  
  pedido.status = "PAGO";
  pedido.payment_id = Math.floor(Date.now() / 1000)

  await producer.connect()
  await producer.send({
    topic: 'ORDEM_PAGA',
    messages: [
      { value: JSON.stringify(pedido)},
    ],
  })

   await producer.disconnect()



}

run().catch(console.error)


