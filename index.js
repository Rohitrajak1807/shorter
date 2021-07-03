require('dotenv').config()
const express = require('express')
const {Client} = require('pg')
const HashRing = require('hashring')
const crypto = require('crypto')
const ring  = new HashRing()
const app = express()

const {SHARD_1_PORT, SHARD_2_PORT, SHARD_3_PORT, USER, PASSWORD, DATABASE} = process.env

const clients = {
    SHARD_1_PORT: new Client({
        host: 'localhost',
        port: SHARD_1_PORT,
        user: 'postgres',
        password: 'password',
        database: 'postgres'
    }),
    SHARD_2_PORT: new Client({
        host: 'localhost',
        port: SHARD_2_PORT,
        user: 'postgres',
        password: 'password',
        database: 'postgres'
    }),
    SHARD_3_PORT: new Client({
        host: 'localhost',
        port: SHARD_3_PORT,
        user: 'postgres',
        password: 'password',
        database: 'postgres'
    })
}

Object.entries(clients).forEach(client => {
    ring.add(client[0])
})

async function connect() {
    Object.entries(clients).forEach(async (client) => {
        try {
            await client[1].connect()
        } catch(e) {
            console.log(e)
            process.exit(1)
        }
    })
}

connect().then(() => {
    console.log('db connected')
})

app.get('/:urlId', async (req, res) => {
    const {urlId} = req.params
    const dbConnection = ring.get(urlId)
    const result = await clients[dbConnection].query('select * from url_table where url_id = $1', [urlId])
    if(result.rowCount < 1) {
        return res.statusCode(404)
    }
    res.send(result.rows[0])
})

app.post('/', async (req, res) => {
    const {url} = req.query
    const hash = crypto.createHash('sha512').update(url).digest('base64')
    const urlId = hash.substr(0, 15)
    const dbConnection = ring.get(urlId)
    await clients[dbConnection].query('insert into url_table (url, url_id) values ($1, $2)', [url, urlId])
    res.send({
        'hash': hash,
        'urlId': urlId,
        'server': dbConnection
    })
})

app.listen(8080, () => {
    console.log('connected to 8080')
})
