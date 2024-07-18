use dotenv::dotenv;
use std::env;
use tokio::net::TcpStream;
use tiberius::{Client, Config, Row};
use tokio_util::compat::TokioAsyncWriteCompatExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv().ok();

    let connection_string = env::var("CONNECTION_STRING").expect("CONNECTION_STRING must be set");
    
    let config = Config::from_ado_string(&connection_string)?;
    let tcp = TcpStream::connect(config.get_addr()).await?;
    tcp.set_nodelay(true)?;

    let mut client = Client::connect(config, tcp.compat_write()).await?;

    let cprice_ids = client.query(r#"
            select CPRICEID from OSC.dbo.CONCEPT_PRICES_HOLD as cph
                inner join OSC.dbo.CONCEPTS as concepts
                    on concepts.CONCEPTID = cph.CONCEPTID
                where
                    concepts.SHORTNAME in
                (
                    'NIOFBBRO',
                    'NIOFBCAR',
                    'NIOFBPREDR',
                    'NIOFBPREFZ',
                    'FBBEKAMA',
                    'FBBEKFOR',
                    'FBBEKMIS',
                    'FBBEKSAN',
                    'FBBEKSEL',
                    'FBCHBOCA',
                    'FBCHBPSL',
                    'FBCHBPTG',
                    'FBCHBSTA',
                    'FBCHRSP',
                    'FBCMAUR',
                    'FBCMCAR',
                    'FBCMSAL',
                    'FBFSDQUI',
                    'FBGFSMIL',
                    'FBMERALA',
                    'FBMERNEW',
                    'FBNICLAS',
                    'FBNICSAL',
                    'FBSHAMAUR',
                    'FBSHAMPHO',
                    'FBSYGILL',
                    'FBSYGKEN',
                    'FBSYGPRY',
                    'FBSYGSAN',
                    'FBSYSACA',
                    'FBSYSATL',
                    'FBSYSCHA',
                    'FBSYSCIL',
                    'FBSYSCOL',
                    'FBSYSDEN',
                    'FBSYSEWI',
                    'FBSYSFAR',
                    'FBSYSGRA',
                    'FBSYSGUL',
                    'FBSYSHAR',
                    'FBSYSIND',
                    'FBSYSJAC',
                    'FBSYSKNO',
                    'FBSYSLIN',
                    'FBSYSMED',
                    'FBSYSMEM',
                    'FBSYSMI',
                    'FBSYSNAS',
                    'FBSYSOCO',
                    'FBSYSRAL',
                    'FBUSFBEN',
                    'FBUSFBOC',
                    'FBUSFBUD',
                    'FBUSFCHA',
                    'FBUSFEVE',
                    'FBUSFFON',
                    'FBUSFGAR',
                    'FBUSFHOU',
                    'FBUSFLOV',
                    'FBUSFMEM',
                    'FBUSFOGD',
                    'FBUSFPHO',
                    'FBUSFPIT',
                    'FBUSFPLY',
                    'FBUSFTRA',
                    'FBUSFWIX',
                    'FBUSFWOO',
                    'INVFBTU',
                    'INVFBW',
                    'FBPFGBOS',
                    'FBPFGCIN',
                    'FBPFGCOA',
                    'FBPFGCOI',
                    'FBPFGDAL',
                    'FBPFGDET',
                    'FBPFGHEN',
                    'FBPFGHIC',
                    'FBPFGHOU',
                    'FBPFGLED',
                    'FBPFGLIV',
                    'FBPFGMIL',
                    'FBPFGMIS',
                    'FBPFGMON',
                    'FBPFGOAK',
                    'FBPFGOMA',
                    'FBPFGORL',
                    'FBPFGPHO',
                    'FBPFGPIS',
                    'FBPFGPOR',
                    'FBPFGRIC',
                    'FBPFGRICE',
                    'FBPFGSOM',
                    'FBPFGSPR',
                    'FBPFGSPRO',
                    'FBPFGTEM',
                    'FBPFGWES',
                    'IBRDCBBIUC',
                    'IBRDCBBICP',
                    'BBIRPFGELK',
                    'BBIRPFGGAI',
                    'BBIRPFGKEN',
                    'BBIRPFGLEB',
                    'BBIRPFGMCK',
                    'BBIRPFGROC',
                    'BBIRPFGSHA'
                )
                and STARTRANGE >= '2024-07-28 00:00:00.000'
            "#, &[&1i32])
        .await?
        .into_first_result()
        .await?
        .into_iter()
        .filter_map(|row: Row| {
            row.get("CPRICEID")
        })
        .collect::<Vec<i32>>();

    println!("Number of records: {}", cprice_ids.len());

    for id in cprice_ids {
        let result = client.execute(format!(r#"
            EXECUTE [OSC].[dbo].[Pricing_DeletePriceHold] {id}
        "#), &[&1i32]).await;

        match result {
            Ok(_) => println!("Successfully deleted Concept Price Hold id: {id}"),
            Err(e) => println!("Error on deleting Concept Price Hold id: {id} Error Message: {e}")
        }
    }

    Ok(())
}
