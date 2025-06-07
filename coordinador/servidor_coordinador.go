package main

import (
	"context"
	"flag"
	"log"
	"net"
	"sync/atomic"

	pb "practica-kv/proto"

	"google.golang.org/grpc"
)

type ServidorCoordinador struct {
	pb.UnimplementedCoordinadorServer

	listaReplicas []string
	indiceRR      uint64
}

// Round-robin para escritura
func (c *ServidorCoordinador) elegirReplicaParaEscritura(clave string) string {
	idx := atomic.AddUint64(&c.indiceRR, 1)
	return c.listaReplicas[int(idx)%len(c.listaReplicas)]
}

// Round-robin para lectura
func (c *ServidorCoordinador) elegirReplicaParaLectura() string {
	idx := atomic.AddUint64(&c.indiceRR, 1)
	return c.listaReplicas[int(idx)%len(c.listaReplicas)]
}

func NewServidorCoordinador(replicas []string) *ServidorCoordinador {
	return &ServidorCoordinador{
		listaReplicas: replicas,
		indiceRR:      0,
	}
}

// ========== RPCs ==========

func (c *ServidorCoordinador) Obtener(ctx context.Context, req *pb.SolicitudObtener) (*pb.RespuestaObtener, error) {
	dir := c.elegirReplicaParaLectura()

	conn, err := grpc.Dial(dir, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	replica := pb.NewReplicaClient(conn)
	return replica.ObtenerLocal(ctx, req)
}

func (c *ServidorCoordinador) Guardar(ctx context.Context, req *pb.SolicitudGuardar) (*pb.RespuestaGuardar, error) {
	dir := c.elegirReplicaParaEscritura(req.Clave)

	conn, err := grpc.Dial(dir, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	replica := pb.NewReplicaClient(conn)
	return replica.GuardarLocal(ctx, req)
}

func (c *ServidorCoordinador) Eliminar(ctx context.Context, req *pb.SolicitudEliminar) (*pb.RespuestaEliminar, error) {
	dir := c.elegirReplicaParaEscritura(req.Clave)

	conn, err := grpc.Dial(dir, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	replica := pb.NewReplicaClient(conn)
	return replica.EliminarLocal(ctx, req)
}

// ========== main ==========

func main() {
	listen := flag.String("listen", ":6000", "dirección donde escucha el Coordinador (ej: :6000)")
	flag.Parse()

	replicas := flag.Args()
	if len(replicas) < 3 {
		log.Fatalf("Debe indicar al menos 3 direcciones de réplicas. Ejemplo:\n go run servidor_coordinador.go -listen :6000 :50051 :50052 :50053")
	}

	coordinador := NewServidorCoordinador(replicas)
	grpcServer := grpc.NewServer()
	pb.RegisterCoordinadorServer(grpcServer, coordinador)

	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatalf("No se pudo escuchar en %s: %v", *listen, err)
	}

	log.Printf("Coordinador escuchando en %s", *listen)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Fallo al servir: %v", err)
	}
}
