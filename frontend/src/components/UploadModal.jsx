/**
 * Upload Modal - NDJSON tick data upload with symbol naming
 */
import { useState, useRef, useCallback } from 'react';
import useAppStore from '../stores/appStore';
import api from '../services/api';

export default function UploadModal({ isOpen, onClose }) {
    const [symbolName, setSymbolName] = useState('');
    const [selectedFile, setSelectedFile] = useState(null);
    const [preview, setPreview] = useState([]);
    const [uploading, setUploading] = useState(false);
    const [error, setError] = useState('');
    const fileInputRef = useRef(null);

    const { addUploadedSymbol, setSelectedSymbol } = useAppStore();

    const handleFileSelect = useCallback((file) => {
        if (!file) return;

        // Validate file type
        if (!file.name.endsWith('.ndjson') && !file.name.endsWith('.json') && !file.name.endsWith('.txt')) {
            setError('Please upload a .ndjson, .json, or .txt file');
            return;
        }

        setSelectedFile(file);
        setError('');

        // Read first 5 lines for preview
        const reader = new FileReader();
        reader.onload = (e) => {
            const content = e.target.result;
            const lines = content.split('\n').filter(l => l.trim()).slice(0, 5);
            const parsed = lines.map(line => {
                try {
                    return JSON.parse(line);
                } catch {
                    return { error: 'Parse error', raw: line.slice(0, 50) };
                }
            });
            setPreview(parsed);
        };
        reader.readAsText(file.slice(0, 10000)); // Read first 10KB for preview
    }, []);

    const handleDrop = useCallback((e) => {
        e.preventDefault();
        const file = e.dataTransfer.files[0];
        handleFileSelect(file);
    }, [handleFileSelect]);

    const handleDragOver = (e) => {
        e.preventDefault();
    };

    const handleUpload = async () => {
        if (!selectedFile || !symbolName.trim()) {
            setError('Please provide both a file and symbol name');
            return;
        }

        setUploading(true);
        setError('');

        try {
            const result = await api.uploadFile(selectedFile, symbolName.trim());

            if (result.error) {
                setError(result.error);
                return;
            }

            // Add to store and select the uploaded symbol
            addUploadedSymbol({
                symbol: result.symbol,
                displayName: symbolName.trim().toUpperCase(),
                tickCount: result.tick_count,
                uploadedAt: Date.now(),
                isUploaded: true
            });

            setSelectedSymbol(result.symbol);
            onClose();

            // Reset form
            setSelectedFile(null);
            setPreview([]);
            setSymbolName('');

        } catch (err) {
            setError(err.message || 'Upload failed');
        } finally {
            setUploading(false);
        }
    };

    if (!isOpen) return null;

    return (
        <div className="modal-overlay" onClick={onClose}>
            <div className="modal-content upload-modal" onClick={e => e.stopPropagation()}>
                <div className="modal-header">
                    <h2>📁 Upload Tick Data</h2>
                    <button className="close-btn" onClick={onClose}>×</button>
                </div>

                <div className="modal-body">
                    {/* Symbol Name Input */}
                    <div className="form-group">
                        <label htmlFor="symbol-name">Symbol Name (unique identifier)</label>
                        <input
                            id="symbol-name"
                            type="text"
                            placeholder="e.g., BTC_HISTORICAL, MY_DATA"
                            value={symbolName}
                            onChange={(e) => setSymbolName(e.target.value.toUpperCase())}
                            className="symbol-input"
                            maxLength={20}
                        />
                        <small className="hint">Will be prefixed with UPLOAD: to avoid conflicts with live data</small>
                    </div>

                    {/* File Drop Zone */}
                    <div
                        className={`file-dropzone ${selectedFile ? 'has-file' : ''}`}
                        onDrop={handleDrop}
                        onDragOver={handleDragOver}
                        onClick={() => fileInputRef.current?.click()}
                    >
                        <input
                            ref={fileInputRef}
                            type="file"
                            accept=".ndjson,.json,.txt"
                            onChange={(e) => handleFileSelect(e.target.files[0])}
                            hidden
                        />

                        {selectedFile ? (
                            <div className="file-info">
                                <span className="file-icon">📄</span>
                                <span className="file-name">{selectedFile.name}</span>
                                <span className="file-size">({(selectedFile.size / 1024).toFixed(1)} KB)</span>
                            </div>
                        ) : (
                            <div className="dropzone-placeholder">
                                <span className="upload-icon">📤</span>
                                <p>Drop NDJSON file here or click to browse</p>
                                <small>Expected format: {"{ symbol, ts, price, size }"}</small>
                            </div>
                        )}
                    </div>

                    {/* File Preview */}
                    {preview.length > 0 && (
                        <div className="file-preview">
                            <h4>Preview (first {preview.length} lines)</h4>
                            <div className="preview-lines">
                                {preview.map((line, i) => (
                                    <pre key={i} className="preview-line">
                                        {JSON.stringify(line, null, 2)}
                                    </pre>
                                ))}
                            </div>
                        </div>
                    )}

                    {/* Error Display */}
                    {error && (
                        <div className="error-message">
                            ⚠️ {error}
                        </div>
                    )}
                </div>

                <div className="modal-footer">
                    <button className="btn-secondary" onClick={onClose}>
                        Cancel
                    </button>
                    <button
                        className="btn-primary"
                        onClick={handleUpload}
                        disabled={uploading || !selectedFile || !symbolName.trim()}
                    >
                        {uploading ? 'Uploading...' : 'Upload & Analyze'}
                    </button>
                </div>
            </div>
        </div>
    );
}
