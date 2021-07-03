const express = require('express')
const {Client} = require('pg')
const HashRing = require('hashring')
const crypto = require('crypto')
const ring  = new HashRing()
const app = express()

const clients = {
    5432: new Client({
        host: 'localhost',
        port: 5432,
        user: 'postgres',
        password: 'password',
        database: 'postgres'
    }),
    5433: new Client({
        host: 'localhost',
        port: 5433,
        user: 'postgres',
        password: 'password',
        database: 'postgres'
    }),
    5434: new Client({
        host: 'localhost',
        port: 5434,
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
        await client[1].connect()
    })
}

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
