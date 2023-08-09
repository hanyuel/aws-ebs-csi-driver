package driver

import (
	"context"
	// "errors"
	"github.com/awslabs/volume-modifier-for-k8s/pkg/rpc"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/kubernetes-sigs/aws-ebs-csi-driver/pkg/driver/internal"
	"github.com/kubernetes-sigs/aws-ebs-csi-driver/pkg/util"
	"k8s.io/klog/v2"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/kubernetes-sigs/aws-ebs-csi-driver/pkg/cloud"
)

func wrapTimeout(t *testing.T, failMessage string, execFunc func()) {
	timeout := time.After(15 * time.Second)
	done := make(chan bool)
	go func() {
		execFunc()
		done <- true
	}()

	select {
	case <-timeout:
		t.Error(failMessage)
	case <-done:
	}
}

func TestBasic(t *testing.T) {
	const NewVolumeType = "gp3"
	const NewSize = 5 * util.GiB

	mockCtl := gomock.NewController(t)
	defer mockCtl.Finish()

	mockCloud := cloud.NewMockCloud(mockCtl)
	mockCloud.EXPECT().ResizeOrModifyDisk(gomock.Any(), gomock.Eq("test-id"), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, volumeID string, newSize int64, options *cloud.ModifyDiskOptions) (int64, error) {
		klog.InfoS("ResizeOrModifyDisk called", "volumeID", volumeID, "newSize", newSize, "options", options)

		// Because there are only two requests, there is only one valid possible options/size
		validOptions := cloud.ModifyDiskOptions{
			VolumeType: NewVolumeType,
		}

		if newSize != NewSize {
			t.Fatalf("newSize incorrect")
		} else if validOptions != *options {
			t.Fatalf("options incorrect")
		}

		return newSize, nil
	})

	awsDriver := controllerService{
		cloud:               mockCloud,
		inFlight:            internal.NewInFlight(),
		driverOptions:       &DriverOptions{},
		modifyVolumeManager: newModifyVolumeManager(),
	}

	var wg sync.WaitGroup
	wg.Add(2)

	go wrapTimeout(t, "ControllerExpandVolume timed out", func() {
		_, err := awsDriver.ControllerExpandVolume(context.Background(), &csi.ControllerExpandVolumeRequest{
			VolumeId: "test-id",
			CapacityRange: &csi.CapacityRange{
				RequiredBytes: NewSize,
			},
		})

		if err != nil {
			t.Error("ControllerExpandVolume returned error")
		}
		wg.Done()
	})
	go wrapTimeout(t, "ModifyVolumeProperties timed out", func() {
		_, err := awsDriver.ModifyVolumeProperties(context.Background(), &rpc.ModifyVolumePropertiesRequest{
			Name: "test-id",
			Parameters: map[string]string{
				ModificationKeyVolumeType: NewVolumeType,
			},
		})

		if err != nil {
			t.Error("ModifyVolumeProperties returned error")
		}
		wg.Done()
	})

	wg.Wait()
}

func TestError(t *testing.T) {
	const NewVolumeType = "gp3"
	const NewSize = 5 * util.GiB

	mockCtl := gomock.NewController(t)
	defer mockCtl.Finish()

	mockCloud := cloud.NewMockCloud(mockCtl)
	mockCloud.EXPECT().ResizeOrModifyDisk(gomock.Any(), gomock.Eq("test-id"), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, volumeID string, newSize int64, options *cloud.ModifyDiskOptions) (int64, error) {
		klog.InfoS("ResizeOrModifyDisk called", "volumeID", volumeID, "newSize", newSize, "options", options)
		// Don't checking input, test returning an error
		return 0, errors.New("modification failed")
	})

	awsDriver := controllerService{
		cloud:               mockCloud,
		inFlight:            internal.NewInFlight(),
		driverOptions:       &DriverOptions{},
		modifyVolumeManager: newModifyVolumeManager(),
	}

	var wg sync.WaitGroup
	wg.Add(2)

	go wrapTimeout(t, "ControllerExpandVolume timed out", func() {
		_, err := awsDriver.ControllerExpandVolume(context.Background(), &csi.ControllerExpandVolumeRequest{
			VolumeId: "test-id",
			CapacityRange: &csi.CapacityRange{
				RequiredBytes: NewSize,
			},
		})

		if err == nil {
			t.Error("ControllerExpandVolume missing error")
		}
		wg.Done()
	})
	go wrapTimeout(t, "ModifyVolumeProperties timed out", func() {
		_, err := awsDriver.ModifyVolumeProperties(context.Background(), &rpc.ModifyVolumePropertiesRequest{
			Name: "test-id",
			Parameters: map[string]string{
				ModificationKeyVolumeType: NewVolumeType,
			},
		})

		if err == nil {
			t.Error("ModifyVolumeProperties missing error")
		}
		wg.Done()
	})

	wg.Wait()
}

func TestTiming(t *testing.T) {
	const NewVolumeType = "gp3"
	const NewSize = 5 * util.GiB
	var modifyVolumeFinished = false

	mockCtl := gomock.NewController(t)
	defer mockCtl.Finish()

	mockCloud := cloud.NewMockCloud(mockCtl)
	mockCloud.EXPECT().ResizeOrModifyDisk(gomock.Any(), gomock.Eq("test-id"), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, volumeID string, newSize int64, options *cloud.ModifyDiskOptions) (int64, error) {
		klog.InfoS("ResizeOrModifyDisk called", "volumeID", volumeID, "newSize", newSize, "options", options)

		// Sleep to simulate ModifyVolume taking a long time
		time.Sleep(5 * time.Second)
		modifyVolumeFinished = true

		return newSize, nil
	})

	awsDriver := controllerService{
		cloud:               mockCloud,
		inFlight:            internal.NewInFlight(),
		driverOptions:       &DriverOptions{},
		modifyVolumeManager: newModifyVolumeManager(),
	}

	var wg sync.WaitGroup
	wg.Add(2)

	go wrapTimeout(t, "ControllerExpandVolume timed out", func() {
		awsDriver.ControllerExpandVolume(context.Background(), &csi.ControllerExpandVolumeRequest{
			VolumeId: "test-id",
			CapacityRange: &csi.CapacityRange{
				RequiredBytes: NewSize,
			},
		})

		if !modifyVolumeFinished {
			t.Error("ControllerExpandVolume returned success BEFORE ResizeOrModifyDisk")
		}

		wg.Done()
	})
	go wrapTimeout(t, "ModifyVolumeProperties timed out", func() {
		awsDriver.ModifyVolumeProperties(context.Background(), &rpc.ModifyVolumePropertiesRequest{
			Name: "test-id",
			Parameters: map[string]string{
				ModificationKeyVolumeType: NewVolumeType,
			},
		})

		if !modifyVolumeFinished {
			t.Error("ModifyVolumeProperties returned success BEFORE ResizeOrModifyDisk")
		}

		wg.Done()
	})

	wg.Wait()
}

func TestSequential(t *testing.T) {
	const NewVolumeType = "gp3"
	const NewSize = 5 * util.GiB

	mockCtl := gomock.NewController(t)
	defer mockCtl.Finish()

	mockCloud := cloud.NewMockCloud(mockCtl)
	mockCloud.EXPECT().ResizeOrModifyDisk(gomock.Any(), gomock.Eq("test-id"), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, volumeID string, newSize int64, options *cloud.ModifyDiskOptions) (int64, error) {
		klog.InfoS("ResizeOrModifyDisk", "volumeID", volumeID, "newSize", newSize, "options", options)
		return newSize, nil
	}).Times(2)

	awsDriver := controllerService{
		cloud:               mockCloud,
		inFlight:            internal.NewInFlight(),
		driverOptions:       &DriverOptions{},
		modifyVolumeManager: newModifyVolumeManager(),
	}

	var wg sync.WaitGroup
	wg.Add(2)

	go wrapTimeout(t, "ControllerExpandVolume timed out", func() {
		klog.InfoS("Calling ControllerExpandVolume")
		_, err := awsDriver.ControllerExpandVolume(context.Background(), &csi.ControllerExpandVolumeRequest{
			VolumeId: "test-id",
			CapacityRange: &csi.CapacityRange{
				RequiredBytes: NewSize,
			},
		})
		klog.InfoS("Finished ControllerExpandVolume")

		if err != nil {
			t.Error("ControllerExpandVolume returned error")
		}
		wg.Done()
	})

	// We expect ModifyVolume to be called by the end of this sleep
	time.Sleep(5 * time.Second)

	go wrapTimeout(t, "ModifyVolumeProperties timed out", func() {
		klog.InfoS("Calling ModifyVolumeProperties")
		_, err := awsDriver.ModifyVolumeProperties(context.Background(), &rpc.ModifyVolumePropertiesRequest{
			Name: "test-id",
			Parameters: map[string]string{
				ModificationKeyVolumeType: NewVolumeType,
			},
		})
		klog.InfoS("Finished ModifyVolumeProperties")

		if err != nil {
			t.Error("ModifyVolumeProperties returned error")
		}
		wg.Done()
	})

	wg.Wait()
}

func TestPartialFail(t *testing.T) {
	const NewVolumeType1 = "gp3"
	const NewVolumeType2 = "io2"
	const NewSize = 5 * util.GiB
	const volumeID = "TestPartialFail"

	mockCtl := gomock.NewController(t)
	defer mockCtl.Finish()

	volumeTypeChosen := ""

	mockCloud := cloud.NewMockCloud(mockCtl)
	mockCloud.EXPECT().ResizeOrModifyDisk(gomock.Any(), gomock.Eq("test-id"), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, volumeID string, newSize int64, options *cloud.ModifyDiskOptions) (int64, error) {
		klog.InfoS("ResizeOrModifyDisk called", "volumeID", volumeID, "newSize", newSize, "options", options)

		if newSize != NewSize {
			t.Errorf("newSize incorrect")
		} else if options.VolumeType == "" {
			t.Errorf("no volume type")
		}

		volumeTypeChosen = options.VolumeType
		return newSize, nil
	})

	awsDriver := controllerService{
		cloud:               mockCloud,
		inFlight:            internal.NewInFlight(),
		driverOptions:       &DriverOptions{},
		modifyVolumeManager: newModifyVolumeManager(),
	}

	var wg sync.WaitGroup
	wg.Add(3)

	type1Error, type2Error := false, false

	go wrapTimeout(t, "ControllerExpandVolume timed out", func() {
		_, err := awsDriver.ControllerExpandVolume(context.Background(), &csi.ControllerExpandVolumeRequest{
			VolumeId: volumeID,
			CapacityRange: &csi.CapacityRange{
				RequiredBytes: NewSize,
			},
		})

		if err != nil {
			t.Error("ControllerExpandVolume returned error")
		}
		wg.Done()
	})
	go wrapTimeout(t, "ModifyVolumeProperties timed out", func() {
		_, err := awsDriver.ModifyVolumeProperties(context.Background(), &rpc.ModifyVolumePropertiesRequest{
			Name: volumeID,
			Parameters: map[string]string{
				ModificationKeyVolumeType: NewVolumeType1, // gp3
			},
		})

		type1Error = err == nil
		wg.Done()
	})
	go wrapTimeout(t, "ModifyVolumeProperties timed out", func() {
		_, err := awsDriver.ModifyVolumeProperties(context.Background(), &rpc.ModifyVolumePropertiesRequest{
			Name: volumeID,
			Parameters: map[string]string{
				ModificationKeyVolumeType: NewVolumeType2, // io2
			},
		})

		type2Error = err == nil
		wg.Done()
	})

	wg.Wait()

	if volumeTypeChosen == NewVolumeType1 {
		if type1Error {
			t.Error("Controller chose", NewVolumeType1, "but errored request")
		}
		if !type2Error {
			t.Error("Controller chose", NewVolumeType1, "but returned success to", NewVolumeType2, "request")
		}
	} else if volumeTypeChosen == NewVolumeType2 {
		if type2Error {
			t.Error("Controller chose", NewVolumeType2, "but errored request")
		}
		if !type1Error {
			t.Error("Controller chose", NewVolumeType2, "but returned success to", NewVolumeType1, "request")
		}
	} else {
		t.Error("No volume type chosen")
	}
}

func TestDuplicateReq(t *testing.T) {
	const NewVolumeType1 = "gp3"
	const NewVolumeType2 = "io2"
	const NewSize = 5 * util.GiB
	const volumeID = "TestDuplicateReq"

	mockCtl := gomock.NewController(t)
	defer mockCtl.Finish()

	mockCloud := cloud.NewMockCloud(mockCtl)
	mockCloud.EXPECT().ResizeOrModifyDisk(gomock.Any(), gomock.Eq("test-id"), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, volumeID string, newSize int64, options *cloud.ModifyDiskOptions) (int64, error) {
		klog.InfoS("ResizeOrModifyDisk called", "volumeID", volumeID, "newSize", newSize, "options", options)

		// if newSize != NewSize {
		// 	t.Errorf("newSize incorrect")
		// } else if options.VolumeType == "" {
		// 	t.Errorf("no volume type")
		// }
		return newSize, nil
	})

	awsDriver := controllerService{
		cloud:               mockCloud,
		inFlight:            internal.NewInFlight(),
		driverOptions:       &DriverOptions{},
		modifyVolumeManager: newModifyVolumeManager(),
	}

	var wg sync.WaitGroup
	wg.Add(2)

	req1Error, req2Error := false, false

	go wrapTimeout(t, "ControllerExpandVolume timed out", func() {
		_, err := awsDriver.ControllerExpandVolume(context.Background(), &csi.ControllerExpandVolumeRequest{
			VolumeId: volumeID,
			CapacityRange: &csi.CapacityRange{
				RequiredBytes: NewSize,
			},
		})

		req1Error = err == nil
		wg.Done()
	})
	go wrapTimeout(t, "ControllerExpandVolume timed out", func() {
		_, err := awsDriver.ControllerExpandVolume(context.Background(), &csi.ControllerExpandVolumeRequest{
			VolumeId: "test-id",
			CapacityRange: &csi.CapacityRange{
				RequiredBytes: NewSize,
			},
		})

		req2Error = err == nil
		wg.Done()
	})
	

	wg.Wait()

	if !req1Error || !req2Error {
		t.Error("both should succeed")
	}
}