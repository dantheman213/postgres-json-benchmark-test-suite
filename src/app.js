const { Pool } = require('pg');
const mongodb = require('mongodb');
const fs = require('fs');
const sizeof = require('object-sizeof');
const uuid = require('node-uuid');

class DatabaseTest {
    static #config = {};
    static #payloadData = [];
    static #postgres = {};
    static #mongodb = {};
    static #maxRecords = 0;

    static async init() {
        this.loadConfig();
        this.getTestData();

        console.log('Waiting for other services to come online...');
        await this.sleep(10000);

        await this.setupPostgresDatabase();
        await this.runPostgresTests();
    }

    static loadConfig() {
        this.#config.DATA_DIR = '/opt/app/data/';
        this.#config.DATA_INSERT_LOOP_COUNT = process.env.DATA_INSERT_LOOP_COUNT;
        this.#config.TEST_ITERATION_COUNT = process.env.TEST_ITERATION_COUNT;
    }

    static getTestData() {
        const dirFiles = fs.readdirSync(this.#config.DATA_DIR);
        const targetFiles = dirFiles.filter((e) => { return e.match(/.*\.(json)/ig); });

        this.#payloadData = [];
        for (const file of targetFiles) {
            this.#payloadData.push(fs.readFileSync(this.#config.DATA_DIR + file));
        }

        this.#maxRecords = this.#config.DATA_INSERT_LOOP_COUNT * (this.#payloadData.length - 1);
        console.log(`Loaded ${this.#payloadData.length} file(s) with ${(sizeof(this.#payloadData) / 1000000).toFixed(2)} megabytes of data!`);
    }

    static async setupPostgresDatabase() {
        console.log('Connecting to Postgres database...');

        this.#postgres.pool = new Pool({
            host: 'postgres',
            user: 'postgres',
            password: 'postgres',
            database: 'postgres',
            port: 5432
        });

        console.log('Resetting Postgres database back to vanilla...');

        try {
            await this.#postgres.pool.query('DROP SCHEMA IF EXISTS public CASCADE');
            await this.#postgres.pool.query('CREATE SCHEMA public');
            await this.#postgres.pool.query('GRANT ALL ON SCHEMA public TO postgres');
            await this.#postgres.pool.query('GRANT ALL ON SCHEMA public TO public');
            await this.#postgres.pool.query('REVOKE USAGE ON SCHEMA public FROM public');
        } catch(e) {
            console.log(e.message);
        }

        console.log('Creating tables for Postgres database...');
        await this.#postgres.pool.query('CREATE table test_json(id SERIAL not null, key text not null, value json not null)');
        await this.#postgres.pool.query('CREATE unique index test_json_id_uindex ON test_json (id)');
        await this.#postgres.pool.query('CREATE unique index test_json_key_uindex ON test_json(key)');
        await this.#postgres.pool.query('ALTER table test_json add constraint test_json_pk primary key(id)');

        await this.#postgres.pool.query('CREATE table test_jsonb(id SERIAL not null, key text not null, value jsonb not null)');
        await this.#postgres.pool.query('CREATE unique index test_jsonb_id_uindex ON test_jsonb (id)');
        await this.#postgres.pool.query('CREATE unique index test_jsonb_key_uindex ON test_jsonb (key)');
        await this.#postgres.pool.query('ALTER table test_jsonb add constraint test_jsonb_pk primary key(id)');
    }

    static async runPostgresTests() {
        this.#postgres.results = {
            json: {
                insertFullBlob: [],
                insertFullBlobAverage: 0,
                partialBlobRead: [],
                partialBlobReadAverage: 0,
                partialBlobWrite: [],
                partialBlobWriteAverage: 0,
                fullBlobRead: [],
                fullBlobReadAverage: 0,
            },
            jsonb: {
                insertFullBlob: [],
                insertFullBlobAverage: 0,
                partialBlobRead: [],
                partialBlobReadAverage: 0,
                partialBlobWrite: [],
                partialBlobWriteAverage: 0,
                fullBlobRead: [],
                fullBlobReadAverage: 0,
            }
        };

        await this.insertJsonBlobTestPostgres();
        await this.readJsonFullBlobTestPostgres();
        await this.writeJsonPartialBlobTestPostgres();
        await this.readJsonPartialBlobTestPostgres();
    }

    static async insertJsonBlobTestPostgres() {
        console.log(`Inserting ${this.#config.DATA_INSERT_LOOP_COUNT * this.#payloadData.length} test items each into two test tables in database -- JSON and JSONB being tested!`);

        for (let i = 0; i < this.#config.DATA_INSERT_LOOP_COUNT; i++) {
            console.log(`Data insert loop ${i+1} of ${this.#config.DATA_INSERT_LOOP_COUNT} executing...`);
            for (const item of this.#payloadData) {
                const id = uuid.v4();

                const startTime1 = new Date();
                await this.#postgres.pool.query('INSERT INTO test_json(key, value) VALUES($1, $2)', [id, item]);
                const endTime1 = new Date();
                this.#postgres.results.json.insertFullBlob.push(endTime1 - startTime1);

                const startTime2 = new Date();
                await this.#postgres.pool.query('INSERT INTO test_jsonb(key, value) VALUES($1, $2)', [id, JSON.parse(item)]);
                const endTime2 = new Date();
                this.#postgres.results.jsonb.insertFullBlob.push(endTime2 - startTime2);
            }
        }

        console.log('Insert Json Blob Test Complete!');
        console.log('Results:');
        this.#postgres.results.json.insertFullBlobAverage = this.average(this.#postgres.results.json.insertFullBlob);
        console.log(`INSERT FULL BLOB JSON AVERAGE: ${this.#postgres.results.json.insertFullBlobAverage}ms`);

        this.#postgres.results.jsonb.insertFullBlobAverage = this.average(this.#postgres.results.jsonb.insertFullBlob);
        console.log(`INSERT FULL BLOB JSONB AVERAGE: ${this.#postgres.results.jsonb.insertFullBlobAverage}ms`);
    }

    static async readJsonFullBlobTestPostgres() {
        console.log('Starting Postgres Read Full JSON blob from row test...!');

        for (let i = 0; i < this.#config.TEST_ITERATION_COUNT; i++) {
            console.log(`Executing test iteration ${i+1} of ${this.#config.TEST_ITERATION_COUNT}`);

            const startTime1 = new Date();
            const result1 = await this.#postgres.pool.query(`SELECT value FROM test_json WHERE id = ${this.generateRandomNumber(1, this.#maxRecords)}`);
            const endTime1 = new Date();
            this.#postgres.results.json.fullBlobRead.push(endTime1 - startTime1);

            const startTime2 = new Date();
            const result2 = await this.#postgres.pool.query(`SELECT value FROM test_jsonb WHERE id = ${this.generateRandomNumber(1, this.#maxRecords)}`);
            const endTime2 = new Date();
            this.#postgres.results.jsonb.fullBlobRead.push(endTime2 - startTime2);
        }

        console.log('Read Full Json Blob Test Complete!');
        console.log('Results:');
        this.#postgres.results.json.fullBlobReadAverage = this.average(this.#postgres.results.json.fullBlobRead);
        console.log(`READ FULL BLOB JSON AVERAGE: ${this.#postgres.results.json.fullBlobReadAverage}ms`);

        this.#postgres.results.jsonb.fullBlobReadAverage = this.average(this.#postgres.results.jsonb.fullBlobRead);
        console.log(`READ FULL BLOB JSONB AVERAGE: ${this.#postgres.results.jsonb.fullBlobReadAverage}ms`);
    }

    static async writeJsonPartialBlobTestPostgres() {
        console.log('Starting Postgres Write Partial JSON blob from row test...!');

        for (let i = 0; i < this.#config.TEST_ITERATION_COUNT; i++) {
            console.log(`Executing test iteration ${i+1} of ${this.#config.TEST_ITERATION_COUNT}`);

            const random2 = this.generateRandomNumber(1, this.#maxRecords);
            const result2 = await this.#postgres.pool.query(`SELECT value FROM test_jsonb WHERE id = ${random2}`);
            const blob2 = result2.rows[0].value;
            const startTime2 = new Date();
            await this.#postgres.pool.query(`UPDATE test_jsonb SET value = jsonb_set(value, '{${this.getRandomPropertyName(blob2)}}'::text[], '"test-value-here"'::jsonb, true) WHERE id = ${random2}`);
            const endTime2 = new Date();
            this.#postgres.results.jsonb.partialBlobWrite.push(endTime2 - startTime2);
        }

        console.log('Write Partial Json Blob Test Complete!');
        console.log('Results:');
        this.#postgres.results.jsonb.partialBlobWriteAverage = this.average(this.#postgres.results.jsonb.partialBlobWrite);
        console.log(`WRITE PARTIAL BLOB JSONB AVERAGE: ${this.#postgres.results.jsonb.partialBlobWriteAverage}ms`);
    }

    static async readJsonPartialBlobTestPostgres() {
        console.log('Starting Postgres Read Partial JSON blob from row test...!');

        for (let i = 0; i < this.#config.TEST_ITERATION_COUNT; i++) {
            console.log(`Executing test iteration ${i+1} of ${this.#config.TEST_ITERATION_COUNT}`);

            const random1 = this.generateRandomNumber(1, this.#maxRecords);
            const result1 = await this.#postgres.pool.query(`SELECT value FROM test_json WHERE id = ${random1}`);
            const blob1 = result1.rows[0].value;
            const startTime1 = new Date();
            await this.#postgres.pool.query(`SELECT value->'${this.getRandomPropertyName(blob1)}' FROM test_json WHERE id = ${random1}`);
            const endTime1 = new Date();
            this.#postgres.results.json.partialBlobRead.push(endTime1 - startTime1);

            const random2 = this.generateRandomNumber(1, this.#maxRecords);
            const result2 = await this.#postgres.pool.query(`SELECT value FROM test_jsonb WHERE id = ${random2}`);
            const blob2 = result2.rows[0].value;
            const startTime2 = new Date();
            await this.#postgres.pool.query(`SELECT value->'${this.getRandomPropertyName(blob2)}' FROM test_jsonb WHERE id = ${random2}`);
            const endTime2 = new Date();
            this.#postgres.results.jsonb.partialBlobRead.push(endTime2 - startTime2);
        }

        console.log('Read Partial Json Blob Test Complete!');
        console.log('Results:');
        this.#postgres.results.json.partialBlobReadAverage = this.average(this.#postgres.results.json.partialBlobRead);
        console.log(`READ PARTIAL BLOB JSON AVERAGE: ${this.#postgres.results.json.partialBlobReadAverage}ms`);

        this.#postgres.results.jsonb.partialBlobReadAverage = this.average(this.#postgres.results.jsonb.partialBlobRead);
        console.log(`READ PARTIAL BLOB JSONB AVERAGE: ${this.#postgres.results.jsonb.partialBlobReadAverage}ms`);
    }

    static setupMongoDatabase() {

    }

    static average(arr) {
        return arr.reduce((p, c) => p + c, 0) / arr.length;
    }

    static sleep(ms) {
        return new Promise(resolve => {
            setTimeout(resolve, ms);
        });
    }

    static generateRandomNumber(min, max) {
        return Math.floor((Math.random() * (max - min)) + min);
    }

    static getRandomPropertyName(obj) {
        const keys = Object.keys(obj);
        return keys[keys.length * Math.random() << 0];
    };
}

DatabaseTest.init();
