import dotenv from 'dotenv';
import chalk from 'chalk';

import { mdb_connect, mysql_connect } from './helpers/dbconnect.js';
import { access2mysql } from './helpers/export.js';

dotenv.config();


async function task() {
    let bdmdb;
    let bdsql;


    mdb_connect(function (status, ret) {
        if (status) {
            bdmdb = ret;

            console.log();
            mysql_connect(function (status, ret) {
                if (status) {
                    bdsql = ret;

                    console.log();
                    access2mysql(bdmdb, bdsql);
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
