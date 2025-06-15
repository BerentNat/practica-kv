package main

import (
	"context"
	"log"
	"time"

	pb "practica-kv/proto"

	"google.golang.org/grpc"
)

func escribir(clave, valor string, puerto string) {
	conn, err := grpc.Dial(puerto, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Error al conectar con %s: %v", puerto, err)
	}
	defer conn.Close()

	cli := pb.NewReplicaClient(conn)

	_, err = cli.GuardarLocal(context.Background(), &pb.SolicitudGuardar{
		Clave: clave,
		Valor: []byte(valor),
	})
	if err != nil {
		log.Printf("Error al guardar en %s: %v", puerto, err)
	} else {
		log.Printf("Guardado en %s: clave=%s valor=%s", puerto, clave, valor)
	}
}

func main() {
	clave := "conflictoX"

	// Ejecutar dos escrituras en paralelo a dos réplicas distintas
	go escribir(clave, "valorA", ":50051")
	go escribir(clave, "valorB", ":50052")

	// Esperar a que terminen
	time.Sleep(3 * time.Second)
}
