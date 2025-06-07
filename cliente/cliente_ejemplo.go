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
	conn, err := grpc.Dial(":6000", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("No se pudo conectar al Coordinador: %v", err)
	}
	defer conn.Close()

	cli := pb.NewCoordinadorClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	clave := "usuario123"
	valor := []byte("datosImportantes")

	// GUARDAR
	log.Printf("Guardando clave %q con valor %q", clave, valor)
	respGuardar, err := cli.Guardar(ctx, &pb.SolicitudGuardar{
		Clave:       clave,
		Valor:       valor,
		RelojVector: nil, // el cliente no aporta reloj
	})
	if err != nil {
		log.Fatalf("Error en Guardar: %v", err)
	}
	fmt.Println("Guardar → OK. Reloj vectorial:", decodeVector(respGuardar.NuevoRelojVector))

	// OBTENER
	respObtener, err := cli.Obtener(ctx, &pb.SolicitudObtener{
		Clave: clave,
	})
	if err != nil {
		log.Fatalf("Error en Obtener: %v", err)
	}
	if !respObtener.Existe {
		log.Fatalf("No se encontró la clave luego de guardar.")
	}
	fmt.Println("Obtener → Valor:", string(respObtener.Valor))
	fmt.Println("             Reloj:", decodeVector(respObtener.RelojVector))

	// ELIMINAR
	respEliminar, err := cli.Eliminar(ctx, &pb.SolicitudEliminar{
		Clave:       clave,
		RelojVector: respObtener.RelojVector,
	})
	if err != nil {
		log.Fatalf("Error en Eliminar: %v", err)
	}
	fmt.Println("Eliminar → OK. Reloj vectorial:", decodeVector(respEliminar.NuevoRelojVector))

	// VERIFICAR QUE NO EXISTA
	respObtener2, err := cli.Obtener(ctx, &pb.SolicitudObtener{
		Clave: clave,
	})
	if err != nil {
		log.Fatalf("Error en Obtener final: %v", err)
	}
	if !respObtener2.Existe {
		fmt.Println("Verificación final: clave eliminada correctamente.")
	} else {
		fmt.Println("Verificación final: la clave todavía existe 😬")
	}
}
