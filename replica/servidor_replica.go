package main

import (
	"context"
	"encoding/binary"
	"log"
	"net"
	"os"
	"strconv"
	"sync"

	pb "practica-kv/proto"

	"google.golang.org/grpc"
)

// ====== Reloj vectorial ======

type VectorReloj [3]uint64

func (vr *VectorReloj) Incrementar(idReplica int) {
	vr[idReplica]++
}

func (vr *VectorReloj) Fusionar(otro VectorReloj) {
	for i := 0; i < 3; i++ {
		if otro[i] > vr[i] {
			vr[i] = otro[i]
		}
	}
}

func (vr VectorReloj) AntesDe(otro VectorReloj) bool {
	menor := false
	for i := 0; i < 3; i++ {
		if vr[i] > otro[i] {
			return false
		}
		if vr[i] < otro[i] {
			menor = true
		}
	}
	return menor
}

func encodeVector(vr VectorReloj) []byte {
	buf := make([]byte, 8*3)
	for i := 0; i < 3; i++ {
		binary.BigEndian.PutUint64(buf[i*8:(i+1)*8], vr[i])
	}
	return buf
}

func decodeVector(b []byte) VectorReloj {
	var vr VectorReloj
	for i := 0; i < 3; i++ {
		vr[i] = binary.BigEndian.Uint64(b[i*8 : (i+1)*8])
	}
	return vr
}

// ====== Estructuras de la réplica ======

type ValorConVersion struct {
	Valor       []byte
	RelojVector VectorReloj
}

type ServidorReplica struct {
	pb.UnimplementedReplicaServer

	mu           sync.Mutex
	almacen      map[string]ValorConVersion
	relojVector  VectorReloj
	idReplica    int
	clientesPeer []pb.ReplicaClient
}

func NewServidorReplica(idReplica int, peerAddrs []string) *ServidorReplica {
	clientes := make([]pb.ReplicaClient, 0, len(peerAddrs))
	for _, addr := range peerAddrs {
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			log.Fatalf("No se pudo conectar con el peer %s: %v", addr, err)
		}
		clientes = append(clientes, pb.NewReplicaClient(conn))
	}

	return &ServidorReplica{
		almacen:      make(map[string]ValorConVersion),
		idReplica:    idReplica,
		clientesPeer: clientes,
	}
}

// ====== RPCs locales ======

func (r *ServidorReplica) GuardarLocal(ctx context.Context, req *pb.SolicitudGuardar) (*pb.RespuestaGuardar, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.relojVector.Incrementar(r.idReplica)
	nuevoReloj := r.relojVector

	r.almacen[req.Clave] = ValorConVersion{
		Valor:       req.Valor,
		RelojVector: nuevoReloj,
	}

	mutacion := &pb.Mutacion{
		Tipo:        pb.Mutacion_GUARDAR,
		Clave:       req.Clave,
		Valor:       req.Valor,
		RelojVector: encodeVector(nuevoReloj),
	}

	// Replicar a los peers
	for _, peer := range r.clientesPeer {
		go func(p pb.ReplicaClient) {
			_, _ = p.ReplicarMutacion(context.Background(), mutacion)
		}(peer)
	}

	return &pb.RespuestaGuardar{
		Exito:            true,
		NuevoRelojVector: encodeVector(nuevoReloj),
	}, nil
}

func (r *ServidorReplica) EliminarLocal(ctx context.Context, req *pb.SolicitudEliminar) (*pb.RespuestaEliminar, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.relojVector.Incrementar(r.idReplica)
	nuevoReloj := r.relojVector

	delete(r.almacen, req.Clave)

	mutacion := &pb.Mutacion{
		Tipo:        pb.Mutacion_ELIMINAR,
		Clave:       req.Clave,
		RelojVector: encodeVector(nuevoReloj),
	}

	for _, peer := range r.clientesPeer {
		go func(p pb.ReplicaClient) {
			_, _ = p.ReplicarMutacion(context.Background(), mutacion)
		}(peer)
	}

	return &pb.RespuestaEliminar{
		Exito:            true,
		NuevoRelojVector: encodeVector(nuevoReloj),
	}, nil
}

func (r *ServidorReplica) ObtenerLocal(ctx context.Context, req *pb.SolicitudObtener) (*pb.RespuestaObtener, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	valor, ok := r.almacen[req.Clave]
	if !ok {
		return &pb.RespuestaObtener{
			Existe: false,
		}, nil
	}

	return &pb.RespuestaObtener{
		Valor:       valor.Valor,
		RelojVector: encodeVector(valor.RelojVector),
		Existe:      true,
	}, nil
}

func (r *ServidorReplica) ReplicarMutacion(ctx context.Context, m *pb.Mutacion) (*pb.Reconocimiento, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	vectorRemoto := decodeVector(m.RelojVector)
	local, existe := r.almacen[m.Clave]

	// Si no existe, o la mutación es más nueva, se aplica
	if !existe || local.RelojVector.AntesDe(vectorRemoto) {
		if m.Tipo == pb.Mutacion_GUARDAR {
			r.almacen[m.Clave] = ValorConVersion{
				Valor:       m.Valor,
				RelojVector: vectorRemoto,
			}
		} else if m.Tipo == pb.Mutacion_ELIMINAR {
			delete(r.almacen, m.Clave)
		}
	}

	r.relojVector.Fusionar(vectorRemoto)

	return &pb.Reconocimiento{
		Ok:             true,
		RelojVectorAck: encodeVector(r.relojVector),
	}, nil
}

// ====== main ======

func main() {
	if len(os.Args) != 5 {
		log.Fatalf("Uso: %s <idReplica> <direccionEscucha> <peer1> <peer2>", os.Args[0])
	}

	idReplica, err := strconv.Atoi(os.Args[1])
	if err != nil || idReplica < 0 || idReplica > 2 {
		log.Fatalf("idReplica debe ser 0, 1 o 2")
	}

	direccion := os.Args[2]
	peers := []string{os.Args[3], os.Args[4]}

	servidor := grpc.NewServer()
	replica := NewServidorReplica(idReplica, peers)
	pb.RegisterReplicaServer(servidor, replica)

	listener, err := net.Listen("tcp", direccion)
	if err != nil {
		log.Fatalf("Error al escuchar en %s: %v", direccion, err)
	}

	log.Printf("Réplica %d escuchando en %s", idReplica, direccion)
	if err := servidor.Serve(listener); err != nil {
		log.Fatalf("Error al servir: %v", err)
	}
}
