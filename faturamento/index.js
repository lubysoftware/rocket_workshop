const { Kafka } = require('kafkajs')

const kafka = new Kafka({
  clientId: 'rocket_app',
  brokers: ['127.0.0.1:9092']
})
const consumer = kafka.consumer({ groupId: 'invoice' })
const producer = kafka.producer()

const run = async () => {

  await consumer.connect()
  await consumer.subscribe({ topic: 'ORDEM_PAGA', fromBeginning: true })

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      var pedido = JSON.parse(message.value.toString());
      await faturarPedido(pedido);

    },
  })
}
const faturarPedido = async (pedido) =>{

  pedido.nfe = "nfe_"+pedido.id+"_2020.xml";
  pedido.status = "FATURADO"
  await producer.connect()
  await producer.send({
    topic: 'ORDEM_FATURADA',
    messages: [
      { value: JSON.stringify(pedido)},
    ],
  })

   await producer.disconnect()

  
}

run().catch(console.error)
