package handlers

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"unbound-future-backend/database"
	"unbound-future-backend/models"
)

// --- Metadata CRUD ---

type CreateMetadataRequest struct {
    ClientName string `json:"client_name" binding:"required"`
    Endpoint   string `json:"endpoint" binding:"required"`
    AK         string `json:"ak" binding:"required"`
    SK         string `json:"sk" binding:"required"`
}

type UpdateMetadataRequest struct {
    ClientName string `json:"client_name"`
    Endpoint   string `json:"endpoint"`
    AK         string `json:"ak"`
    SK         string `json:"sk"`
}

func CreateMetadata(c *gin.Context) {
    var req CreateMetadataRequest
    if err := c.ShouldBindJSON(&req); err != nil {
        c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
        return
    }
    
    // Simulate encryption
    skEncrypted := "enc_" + req.SK
    
    meta := models.TransferMetadata{
        ClientName: req.ClientName,
        Endpoint:   req.Endpoint,
        AK:         req.AK,
        SKEncrypted: skEncrypted,
    }
    
    if err := database.DB.Create(&meta).Error; err != nil {
        c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
        return
    }
    
    c.JSON(http.StatusCreated, meta)
}

func ListMetadata(c *gin.Context) {
    var metas []models.TransferMetadata
    page, _ := strconv.Atoi(c.DefaultQuery("skip", "0"))
    limit, _ := strconv.Atoi(c.DefaultQuery("limit", "100"))
    
    if err := database.DB.Offset(page).Limit(limit).Find(&metas).Error; err != nil {
        c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
        return
    }
    
    c.JSON(http.StatusOK, metas)
}

func GetMetadata(c *gin.Context) {
    id := c.Param("id")
    var meta models.TransferMetadata
    
    if err := database.DB.First(&meta, id).Error; err != nil {
        c.JSON(http.StatusNotFound, gin.H{"error": "Metadata not found"})
        return
    }
    
    c.JSON(http.StatusOK, meta)
}

func UpdateMetadata(c *gin.Context) {
    id := c.Param("id")
    var req UpdateMetadataRequest
    if err := c.ShouldBindJSON(&req); err != nil {
        c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
        return
    }
    
    var meta models.TransferMetadata
    if err := database.DB.First(&meta, id).Error; err != nil {
        c.JSON(http.StatusNotFound, gin.H{"error": "Metadata not found"})
        return
    }
    
    if req.ClientName != "" {
        meta.ClientName = req.ClientName
    }
    if req.Endpoint != "" {
        meta.Endpoint = req.Endpoint
    }
    if req.AK != "" {
        meta.AK = req.AK
    }
    if req.SK != "" {
        meta.SKEncrypted = "enc_" + req.SK
    }
    
    if err := database.DB.Save(&meta).Error; err != nil {
        c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
        return
    }
    
    c.JSON(http.StatusOK, meta)
}

func DeleteMetadata(c *gin.Context) {
    id := c.Param("id")
    var meta models.TransferMetadata
    if err := database.DB.First(&meta, id).Error; err != nil {
         c.JSON(http.StatusNotFound, gin.H{"error": "Metadata not found"})
         return
    }
    
    if err := database.DB.Delete(&meta).Error; err != nil {
        c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
        return
    }
    
    c.JSON(http.StatusOK, gin.H{"ok": true})
}
