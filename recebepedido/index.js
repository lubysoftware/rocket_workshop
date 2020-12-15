const express = require('express')
const bodyParser = require('body-parser');
const app = express()
const { Kafka } = require('kafkajs')
app.use(bodyParser.json());
const port = 3001


const kafka = new Kafka({
  clientId: 'rocket_app',
  brokers: ['localhost:9092']
})



app.get('/', (req, res) => {
  res.send('Hello, Api de Pedido')
})
app.post('/recebepedido', async (req, res) => {
 
  var pedido = req.body;

  pedido.id = Math.floor(Date.now() / 1000)

  pedido.status = "PENDENTE_PAGAMENTO"


    const producer = kafka.producer()
    await producer.connect()
    await producer.send({
      topic: 'ORDEM_RECEBIDA',
      messages: [
        { value: JSON.stringify(pedido)},
      ],
    })

    await producer.disconnect()




  return res.json({'order':pedido, 'message':"Pedido realizado com sucesso. Estamos analisando seu pagamento. Você receberá informações em breve."})
 
})
app.listen(port, () => {
  console.log(`Express API http://localhost:${port}`)
})
