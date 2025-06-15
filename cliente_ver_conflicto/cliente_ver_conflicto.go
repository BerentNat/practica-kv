package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"time"

	pb "practica-kv/proto"

	"google.golang.org/grpc"
)

func decodeVector(b []byte) [3]uint64 {
	var vr [3]uint64
	for i := 0; i < 3; i++ {
		vr[i] = binary.BigEndian.Uint64(b[i*8 : (i+1)*8])
	}
	return vr
}

func main() {
	// Conectarse al Coordinador
	conn, err := grpc.Dial(":6000", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("No se pudo conectar al Coordinador: %v", err)
	}
	defer conn.Close()

	cli := pb.NewCoordinadorClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Clave que fue modificada en conflicto
	clave := "conflictoX"

	// Obtener valor actual
	resp, err := cli.Obtener(ctx, &pb.SolicitudObtener{
		Clave: clave,
	})
	if err != nil {
		log.Fatalf("Error al obtener clave: %v", err)
	}

	if !resp.Existe {
		fmt.Println("⚠️  La clave conflictoX no existe.")
	} else {
		fmt.Printf("✅ Valor actual: %s\n", string(resp.Valor))
		fmt.Printf("🕒 Reloj vectorial: %v\n", decodeVector(resp.RelojVector))
	}
}
