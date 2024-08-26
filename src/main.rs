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
                    'BBIFPFGELK',
                    'BBIFPFGGAI',
                    'BBIFPFGKEN',
                    'BBIFPFGLEB',
                    'BBIFPFGMCK',
                    'BBIFPFGROC',
                    'BBIFPFGSHA'
                )
                and STARTRANGE = '2024-08-04'
                and ENDRANGE = '2024-08-31';
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
