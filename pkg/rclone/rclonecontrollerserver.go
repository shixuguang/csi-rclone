/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package rclone

import (
	"fmt"
	"os/exec"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
	csicommon "github.com/kubernetes-csi/drivers/pkg/csi-common"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog"
)

type RcloneControllerServer struct {
	*csicommon.DefaultControllerServer
}

func NewRcloneControllerServer(d *csicommon.CSIDriver) *RcloneControllerServer {
	return &RcloneControllerServer{
		DefaultControllerServer: &csicommon.DefaultControllerServer{Driver: d},
	}
}

func (cs *RcloneControllerServer) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	name := req.GetName()
	parameters := req.GetParameters()
	secrets := req.GetSecrets()
	if secrets == nil || len(secrets) == 0 {
		klog.Infof("provision secret not avaliable, using default")
		var err error
		secrets, err = getSecrets(default_secret_name)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to retrieve secret rclone-secret: %v", err.Error())
		}
	}
	remote, remotePath, flags, e := extractFlags(name, parameters, secrets)
	if e != nil {
		klog.Warningf("storage parameter error: %s", e)
		return nil, e
	}

	// extra info for volume deletion can be saved in cm

	/*
		if err := createVolumeMc(remote, remotePath, flags); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to create volume %s: %v", name, err.Error())
		}*/

	// policy set doesn't work
	if err := volumeOperation("mkdir", remote, remotePath, flags); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create volume %s: %v", name, err.Error())
	}

	vol := &csi.Volume{
		CapacityBytes: 0, // by setting it to zero, Provisioner will use PVC requested size as PV size
		VolumeId:      remotePath,
		VolumeContext: parameters,
	}

	return &csi.CreateVolumeResponse{Volume: vol}, nil
}

func (cs *RcloneControllerServer) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	klog.Infof("DeleteVolume: called with args %+v", *req)
	// secrets maybe shuffled after a while
	secrets := req.GetSecrets()
	if secrets == nil || len(secrets) == 0 {
		klog.Infof("provision secret not avaliable, using default")
		var err error
		secrets, err = getSecrets(default_secret_name)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to retrieve secret rclone-secret: %v", err.Error())
		}
	}

	remote, remotePath, flags, e := extractFlags(req.GetVolumeId(), map[string]string{}, secrets)
	if e != nil {
		klog.Warningf("storage parameter error: %s", e)
		return nil, e
	}

	volId := req.GetVolumeId()
	if err := volumeOperation("purge", remote, remotePath, flags); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create volume %s: %v", volId, err.Error())
	}

	return &csi.DeleteVolumeResponse{}, nil
}

func volumeOperation(volumeCmd string, remote string, remotePath string, flags map[string]string) error {
	rcloneCmd := "rclone"
	cmdArgs := []string{}

	defaultFlags := map[string]string{}

	// rclone volumeCmd remote:path /path/to/mountpoint [flags]
	cmdArgs = append(
		cmdArgs,
		volumeCmd,
		fmt.Sprintf(":%s:%s", remote, remotePath),
		"--no-check-certificate",
	)

	// Add default flags
	for k, v := range defaultFlags {
		// Exclude overriden flags
		if _, ok := flags[k]; !ok {
			cmdArgs = append(cmdArgs, fmt.Sprintf("--%s=%s", k, v))
		}
	}

	// Add user supplied flags
	for k, v := range flags {
		cmdArgs = append(cmdArgs, fmt.Sprintf("--%s=%s", k, v))
	}

	if volumeCmd == "mkdir" { // has no effect
		cmdArgs = append(cmdArgs, fmt.Sprintf("--%s=%s", "s3-bucket-acl", "public-read-write"))
		cmdArgs = append(cmdArgs, fmt.Sprintf("--%s=%s", "s3-acl", "public-read-write"))
	}

	klog.Infof("executing %s command cmd=%s, remote=:%s:%s", volumeCmd, rcloneCmd, remote, remotePath)
	klog.Infof("args=%s", strings.Join(cmdArgs, " "))

	out, err := exec.Command(rcloneCmd, cmdArgs...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("rclone operation failed: %v cmd: '%s' operation: '%s' remote: ':%s:%s' output: %q",
			err, rcloneCmd, volumeCmd, remote, remotePath, string(out))
	}

	return nil
}

func createVolumeMc(remote string, remotePath string, flags map[string]string) error {

	mcCmd := "mc"

	//mc alias set ${alias-name} ${s3-endpoint} ${s3-access-key-id} ${s3-secret-access-key} --insecure
	strArgs := fmt.Sprintf("alias set %s %s %s %s --insecure", remote, flags["s3-endpoint"], flags["s3-access-key-id"], flags["s3-secret-access-key"])
	cmdArgs := strings.Split(strArgs, " ")

	klog.Infof("executing %s %s", mcCmd, strArgs)
	out, err := exec.Command(mcCmd, cmdArgs...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("mc alias set failed, err: %v output: %q", err, string(out))
	}

	//mc mb $alias/$bucketname --insecure
	strArgs = fmt.Sprintf("mb %s/%s --insecure", remote, remotePath)
	cmdArgs = strings.Split(strArgs, " ")

	klog.Infof("executing %s %s", mcCmd, strArgs)
	out, err = exec.Command(mcCmd, cmdArgs...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s %s failed, err: %v output: %q", mcCmd, strArgs, err, string(out))
	}

	//mc policy set public $alias/$bucketname --insecure
	strArgs = fmt.Sprintf("policy set public %s/%s --insecure", remote, remotePath)
	cmdArgs = strings.Split(strArgs, " ")

	klog.Infof("executing %s %s", mcCmd, strArgs)
	out, err = exec.Command(mcCmd, cmdArgs...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s %s failed, err: %v output: %q", mcCmd, strArgs, err, string(out))
	}

	return nil
}

// determined by attachRequired flag in CSIDriver(storage.k8s.io/v1) object, and if not created by ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME, actual call triggered from master controller
func (cs *RcloneControllerServer) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func (cs *RcloneControllerServer) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

// support of csi 1.5 spec
func (cs *RcloneControllerServer) ControllerExpandVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func (cs *RcloneControllerServer) ControllerGetVolume(ctx context.Context, req *csi.ControllerGetVolumeRequest) (*csi.ControllerGetVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}
