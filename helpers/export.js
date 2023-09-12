import chalk from 'chalk';

export async function access2mysql(bdmdb, bdsql) {

    console.log("Cria tabela de logs");
    await bdsql.schema.dropTableIfExists('#logs');
    await bdsql.schema.createTable('#logs', function (table) {
        table.increments('id').primary();
        table.string('type');
        table.string('table_name');
        table.longtext('message');
        table.longtext('columns');
        table.timestamp('created_at').defaultTo(bdsql.raw('CURRENT_TIMESTAMP'));
        table.timestamp('updated_at').defaultTo(bdsql.raw('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'));
    })

    console.log(chalk.bold("Percorrer tabelas do banco de dados Access"))

    const tables = bdmdb.getTableNames();

    async function exportTables() {
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
                    message: 'Registros inseridos: ' + data.length,
                    columns: JSON.stringify(columns),
                });
            } catch (error) {
                console.error(chalk.red("Tabela:"), tablename);
                console.error(chalk.red(`Erro ao criar/inserir dados na tabela:`, error));

                await bdsql('#logs').insert({
                    type: 'error',
                    table_name: tablename,
                    message: JSON.stringify(error),
                    columns: JSON.stringify(columns),
                });
            }
        }

        console.log(chalk.green("Fim do processo de importação"))
    }

    exportTables();

}