package handlers

import (
	"net/http"

	"unbound-future-backend/database"
	"unbound-future-backend/models"

	"github.com/gin-gonic/gin"
)

// GetWorkerCookieConfig 根据机器名查询启用的 cookie 配置
// GET /api/worker-cookie-configs?machine_name=xxx
func GetWorkerCookieConfig(c *gin.Context) {
	machineName := c.Query("machine_name")
	if machineName == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "machine_name parameter is required"})
		return
	}

	var config models.WorkerCookieConfig
	err := database.DB.Where("machine_name = ? AND enabled = ?", machineName, true).First(&config).Error
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "No enabled cookie config found for this machine"})
		return
	}

	c.JSON(http.StatusOK, config)
}

// ListWorkerMachineNames 获取所有机器名列表
// GET /api/worker-cookie-configs/machine-names
func ListWorkerMachineNames(c *gin.Context) {
	var configs []models.WorkerCookieConfig
	err := database.DB.Select("machine_name").Where("enabled = ?", true).Find(&configs).Error
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	machineNames := make([]string, 0, len(configs))
	for _, config := range configs {
		machineNames = append(machineNames, config.MachineName)
	}

	c.JSON(http.StatusOK, gin.H{"machine_names": machineNames})
}
