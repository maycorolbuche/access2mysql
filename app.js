import dotenv from 'dotenv';
import fs from 'fs';
import MDBReader from "mdb-reader";
import knex from "knex"

import chalk from 'chalk';

dotenv.config();

function mdb_connect(callback = function () { }) {

    console.log("Dados de conexão")
    console.log(chalk.blue("ACCESS_URL"), process.env.ACCESS_URL)

    try {
        const buffer = fs.readFileSync(process.env.ACCESS_URL);
        const bdmdb = new MDBReader(buffer);
        callback(true, bdmdb);
    } catch (error) {
        callback(false, error);
    }
}

async function mysql_connect(callback = function () { }) {

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

        //Criar tabela de logs
        await bdsql.schema.dropTableIfExists('#logs');
        await bdsql.schema.createTable('#logs', function (table) {
            table.increments('id').primary();
            table.string('type');
            table.string('table_name');
            table.longtext('message');
            table.timestamp('created_at').defaultTo(bdsql.raw('CURRENT_TIMESTAMP'));
            table.timestamp('updated_at').defaultTo(bdsql.raw('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'));
        })

        callback(true, bdsql);
    } catch (error) {
        console.error('Erro ao conectar ao banco de dados:', error);
        callback(false, error);
    }
}

function export_data(bdmdb, bdsql) {

    console.log(chalk.bold("Percorrer tabelas do banco de dados Access"))

    const tables = bdmdb.getTableNames();

    async function createTables() {
        for (const tablename of tables) {
            const table = bdmdb.getTable(tablename);
            const columns = table.getColumns();

            try {
                // Deleta a tabela, se existir
                await bdsql.schema.dropTableIfExists(tablename);

                // Verifique se a tabela existe e, se não, crie-a
                const exists = await bdsql.schema.hasTable(tablename);
                if (!exists) {
                    await bdsql.schema.createTable(tablename, function (table) {
                        columns.forEach(column => {
                            let { name, type, size, nullable, autoLong, autoUUID } = column;
                            switch (type) {
                                case 'memo':
                                    type = 'longtext';
                                    break;
                                case 'long':
                                    type = 'integer';
                                    break;
                            }

                            const columnBuilder = table[type](name, size);

                            if (nullable && !autoLong) {
                                columnBuilder.nullable();
                            } else {
                                columnBuilder.notNullable();
                            }

                            if (autoLong && type == "int") {
                                columnBuilder.increments();
                            }
                            if (autoLong) {
                                columnBuilder.primary();
                            }

                            if (autoUUID) {
                                columnBuilder.uuid();
                            }
                        });
                    });
                    console.log(chalk.blue("Tabela:"), tablename);
                    console.log(`Tabela ${tablename} criada com sucesso.`);
                }

                const data = table.getData();
                console.log(`Inserindo dados na tabela ${tablename} - Qtd. registros: `, data.length);

                if (data.length > 0) {
                    await bdsql(tablename)
                        .insert(data);
                    console.log(`Dados inseridos com sucesso na tabela ${tablename}.`);
                } else {
                    console.log(`Não há dados para serem inseridos na tabela ${tablename}`);
                }

                await bdsql('#logs').insert({
                    type: 'info',
                    table_name: tablename,
                    message: 'Registros inseridos: ' + data.length
                });
            } catch (error) {
                console.error(chalk.red("Tabela:"), tablename);
                console.error(chalk.red(`Erro ao criar/inserir dados na tabela:`, error));

                await bdsql('#logs').insert({
                    type: 'error',
                    table_name: tablename,
                    message: JSON.stringify(error)
                });
            }
        }

        console.log(chalk.green("Fim do processo de importação"))
    }

    createTables();

}

async function task() {
    let bdmdb;
    let bdsql;

    console.log(chalk.bold("Conectando ao banco de dados Access"))
    mdb_connect(function (status, ret) {
        if (status) {
            bdmdb = ret;

            console.log(chalk.green("Conectado"))

            console.log();
            console.log(chalk.bold("Conectando ao banco de dados MySQL"))
            mysql_connect(function (status, ret) {
                if (status) {
                    bdsql = ret;

                    console.log(chalk.green("Conectado"))

                    console.log();
                    export_data(bdmdb, bdsql);
                } else {
                    console.error(chalk.red(ret))
                }
            });

        } else {
            console.error(chalk.red(ret))
        }
    });

}

task();
