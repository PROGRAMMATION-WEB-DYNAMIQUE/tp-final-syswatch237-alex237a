// src/server.rs
use crate::commands;
use crate::logging::{info, error, debug};
use crate::system::SystemSnapshot;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;

const AUTH_TOKEN: &str = "ENSPD2026";

pub fn start_server(snapshot: Arc<Mutex<SystemSnapshot>>) -> std::io::Result<()> {
    let listener = TcpListener::bind("0.0.0.0:7878")?;
    info!("Serveur en écoute sur port 7878");

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let snap = Arc::clone(&snapshot);
                thread::spawn(move || handle_client(stream, snap));
            }
            Err(e) => error!("Erreur connexion entrante: {}", e),
        }
    }
    Ok(())
}

fn handle_client(mut stream: TcpStream, snapshot: Arc<Mutex<SystemSnapshot>>) {
    let peer = stream.peer_addr().unwrap();
    info!("[+] Connexion de {}", peer);

    // Étape 1 : authentification
    if let Err(e) = authenticate(&mut stream) {
        error!("[!] Échec auth de {} : {}", peer, e);
        let _ = stream.write_all(b"AUTH_FAILED\n");
        return;
    }
    info!("[✓] Authentifié: {}", peer);

    let mut reader = BufReader::new(stream.try_clone().expect("clone"));
    let mut response_writer = stream;

    loop {
        // Envoi du prompt (optionnel, mais le master n'en a pas besoin)
        // On attend une ligne de commande
        let mut cmd_line = String::new();
        match reader.read_line(&mut cmd_line) {
            Ok(0) => break, // EOF
            Ok(_) => {
                let cmd = cmd_line.trim();
                debug!("Commande de {}: '{}'", peer, cmd);

                if cmd.eq_ignore_ascii_case("quit") {
                    let _ = response_writer.write_all(b"BYE\n");
                    break;
                }

                let response = {
                    let snap = snapshot.lock().unwrap();
                    commands::execute(&snap, cmd)
                };
                let _ = response_writer.write_all(response.as_bytes());
                let _ = response_writer.write_all(b"END\n");
                let _ = response_writer.flush();
            }
            Err(e) => {
                error!("Erreur lecture de {} : {}", peer, e);
                break;
            }
        }
    }

    info!("[-] Déconnexion de {}", peer);
}

fn authenticate(stream: &mut TcpStream) -> Result<(), &'static str> {
    // Envoi du challenge "TOKEN: \n"
    stream.write_all(b"TOKEN: \n").map_err(|_| "write failed")?;
    stream.flush().map_err(|_| "flush failed")?;

    let mut reader = BufReader::new(stream.try_clone().map_err(|_| "clone failed")?);
    let mut token = String::new();
    reader.read_line(&mut token).map_err(|_| "read failed")?;
    if token.trim() != AUTH_TOKEN {
        return Err("bad token");
    }
    stream.write_all(b"OK\n").map_err(|_| "write failed")?;
    stream.flush().map_err(|_| "flush failed")?;
    Ok(())
}
