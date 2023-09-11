import fs from 'fs';
import MDBReader from "mdb-reader";
import dotenv from 'dotenv';
import knex from "knex";
import chalk from 'chalk';

dotenv.config();

export function mdb_connect(callback = function () { }) {
    console.log(chalk.bold("Conectando ao banco de dados Access"))
    console.log("Dados de conexão")
    console.log(chalk.blue("ACCESS_URL"), process.env.ACCESS_URL)

    try {
        const buffer = fs.readFileSync(process.env.ACCESS_URL);
        const bdmdb = new MDBReader(buffer);
        console.log(chalk.green("Conectado"))
        callback(true, bdmdb);
    } catch (error) {
        callback(false, error);
    }
}

export async function mysql_connect(callback = function () { }) {
    console.log(chalk.bold("Conectando ao banco de dados MySQL"))
    console.log("Dados de conexão")
    console.log(chalk.blue("MYSQL_HOST"), process.env.MYSQL_HOST)
    console.log(chalk.blue("MYSQL_USER"), process.env.MYSQL_USER)
    console.log(chalk.blue("MYSQL_PASSWORD"), "******")
    console.log(chalk.blue("MYSQL_DATABASE"), process.env.MYSQL_DATABASE)

    try {
        const bdsql = knex({
            client: 'mysql2',
            connection: {
                host: process.env.MYSQL_HOST,
                user: process.env.MYSQL_USER,
                password: process.env.MYSQL_PASSWORD,
                database: process.env.MYSQL_DATABASE
            }
        });

        // Teste a conexão chamando um método simples do Knex, como 'raw'
        await bdsql.raw('SELECT 1');

        console.log(chalk.green("Conectado"))
        callback(true, bdsql);
    } catch (error) {
        console.error('Erro ao conectar ao banco de dados:', error);
        callback(false, error);
    }
}
