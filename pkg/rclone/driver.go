package rclone

import (
	"github.com/container-storage-interface/spec/lib/go/csi"
	csicommon "github.com/kubernetes-csi/drivers/pkg/csi-common"
	"k8s.io/klog"
)

type driver struct {
	csiDriver *csicommon.CSIDriver
	endpoint  string

	ns *nodeServer
	// add node service cap
	nscap []*csi.NodeServiceCapability
}

var (
	DriverName    = "csi-rclone"
	DriverVersion = "latest"
)

func NewDriver(nodeID, endpoint string) *driver {
	klog.Infof("Starting new %s driver in version %s", DriverName, DriverVersion)

	d := &driver{}

	d.endpoint = endpoint

	d.csiDriver = csicommon.NewCSIDriver(DriverName, DriverVersion, nodeID)
	d.csiDriver.AddVolumeCapabilityAccessModes([]csi.VolumeCapability_AccessMode_Mode{csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER})
	d.csiDriver.AddControllerServiceCapabilities([]csi.ControllerServiceCapability_RPC_Type{csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME})
	// not used for now
	d.AddNodeServiceCapabilities([]csi.NodeServiceCapability_RPC_Type{
		//csi.NodeServiceCapability_RPC_GET_VOLUME_STATS, // size not applicable to minio
		//csi.NodeServiceCapability_RPC_SINGLE_NODE_MULTI_WRITER,
	})

	return d
}

func NewNodeServer(d *driver) *nodeServer {
	return &nodeServer{
		DefaultNodeServer: csicommon.NewDefaultNodeServer(d.csiDriver),
	}
}

func NewNodeServiceCapability(cap csi.NodeServiceCapability_RPC_Type) *csi.NodeServiceCapability {
	return &csi.NodeServiceCapability{
		Type: &csi.NodeServiceCapability_Rpc{
			Rpc: &csi.NodeServiceCapability_RPC{
				Type: cap,
			},
		},
	}
}

func (d *driver) AddNodeServiceCapabilities(nl []csi.NodeServiceCapability_RPC_Type) {
	var nsc []*csi.NodeServiceCapability
	for _, n := range nl {
		klog.Infof("Enabling node service capability: %v", n.String())
		nsc = append(nsc, NewNodeServiceCapability(n))
	}
	d.nscap = nsc
}

func (d *driver) Run() {
	s := csicommon.NewNonBlockingGRPCServer()
	s.Start(d.endpoint,
		csicommon.NewDefaultIdentityServer(d.csiDriver),
		NewRcloneControllerServer(d.csiDriver),
		NewNodeServer(d))
	s.Wait()
}
