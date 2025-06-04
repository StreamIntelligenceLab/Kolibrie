/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use kolibrie::utils::*;

/*
 * Example of POST INSERT: curl -X POST -H "Content-Type: application/sparql-update" --data "INSERT { s p o }" http://localhost:7878/
 * Example of GET SELECT: curl "http://localhost:7878/?query=s%20p%20o"
 */

fn main() {
    run_server();
    // let mut db = SparqlDatabase::new();
    // db.handle_update("INSERT { s p o }");
    // db.debug_print_triples(); // Add this line
}
