package chronicle

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"gecgithub01.walmart.com/auk000v/chronicle/protocol"
	"gecgithub01.walmart.com/auk000v/chronicle/store"
)

func (h *Handler) snapshotStore() (store.ProjectionSnapshotStore, bool) {
	s, ok := h.Store.(store.ProjectionSnapshotStore)
	_, paged := h.Store.(store.PageReader)
	return s, h.EnableSnapshots && ok && paged
}

func (h *Handler) snapshotHeaders(w http.ResponseWriter, incarnation string) {
	if _, enabled := h.snapshotStore(); enabled {
		w.Header().Set(protocol.HeaderStreamSnapshot, "v1")
		w.Header().Set(protocol.HeaderStreamIncarnation, incarnation)
		w.Header().Set(protocol.HeaderStreamSnapshotMaxBytes, strconv.Itoa(store.MaxSnapshotBytes))
		w.Header().Set(protocol.HeaderStreamSnapshotMaxTotalBytes, strconv.Itoa(store.MaxSnapshotTotalBytes))
		w.Header().Set(protocol.HeaderStreamSnapshotMaxVersions, strconv.Itoa(store.MaxSnapshotVersions))
	}
}

// snapshotReadGuard is checked against the authoritative first page, not a
// separate HEAD preflight. Existing page and live-read fences then preserve it.
func (h *Handler) snapshotReadGuard(r *http.Request) (string, error) {
	if len(r.Header.Values(protocol.HeaderIfStreamIncarnation)) == 0 {
		return "", nil
	}
	if _, enabled := h.snapshotStore(); !enabled {
		return "", newHTTPError(http.StatusNotImplemented, "projection snapshots are not enabled on this backend")
	}
	value, err := snapshotHeader(r, protocol.HeaderIfStreamIncarnation, true)
	if err != nil {
		return "", err
	}
	if strings.ContainsAny(value, " ,\t\r\n") {
		return "", newHTTPError(http.StatusBadRequest, "invalid stream incarnation")
	}
	return value, nil
}

func (h *Handler) handleSnapshot(w http.ResponseWriter, r *http.Request, path string, query url.Values) error {
	// Never let an unsupported snapshot PUT fall through to stream creation.
	backend, enabled := h.snapshotStore()
	if !enabled {
		return newHTTPError(http.StatusNotImplemented, "projection snapshots are not enabled on this backend")
	}
	w.Header().Set(protocol.HeaderStreamSnapshot, "v1")
	w.Header().Set("Cache-Control", "private, no-store")
	switch r.Method {
	case http.MethodGet:
		if _, err := h.authorizeRead(r, path); err != nil {
			return err
		}
	case http.MethodPut, http.MethodDelete:
		if err := h.authorizeSnapshotPublish(r, path); err != nil {
			return err
		}
	default:
		w.Header().Set("Allow", "GET, PUT, DELETE, OPTIONS")
		return newHTTPError(http.StatusMethodNotAllowed, "snapshot resources support GET, PUT and DELETE")
	}
	values := query["snapshot"]
	if len(query) != 1 || len(values) != 1 || store.ValidateSnapshotProjection(values[0]) != nil {
		return newHTTPError(http.StatusBadRequest, "snapshot requires one valid projection and no other query parameters")
	}
	projection := values[0]
	if r.Method == http.MethodDelete {
		inc, err := snapshotHeader(r, protocol.HeaderIfStreamIncarnation, true)
		if err != nil || strings.ContainsAny(inc, " ,\t\r\n") {
			if err != nil {
				return err
			}
			return newHTTPError(http.StatusBadRequest, "invalid stream incarnation")
		}
		version, err := snapshotHeader(r, protocol.HeaderStreamSnapshot, true)
		if err != nil {
			return err
		}
		match, err := snapshotHeader(r, "If-Match", false)
		if err != nil {
			return err
		}
		if match == "" {
			return newHTTPError(http.StatusPreconditionRequired, "retirement requires If-Match")
		}
		if version != "v1" || !strongSnapshotETag(match) || len(r.Header.Values("If-None-Match")) != 0 {
			return newHTTPError(http.StatusBadRequest, "retirement requires v1 and one strong If-Match")
		}
		if err := backend.DeleteSnapshot(r.Context(), path, projection, inc, match); err != nil {
			return snapshotHTTPError(err)
		}
		h.snapshotHeaders(w, inc)
		w.WriteHeader(http.StatusNoContent)
		return nil
	}
	if r.Method == http.MethodGet {
		image, err := backend.GetSnapshot(r.Context(), path, projection)
		if err != nil {
			return snapshotHTTPError(err)
		}
		h.snapshotHeaders(w, image.Incarnation)
		w.Header().Set(protocol.HeaderStreamSnapshotOffset, image.Offset.String())
		w.Header().Set("Content-Type", image.ContentType)
		w.Header().Set("ETag", image.ETag)
		w.Header().Set(protocol.HeaderContentDigest, snapshotDigest(image.Body))
		w.Header().Set("Content-Length", strconv.Itoa(len(image.Body)))
		w.WriteHeader(http.StatusOK)
		_, err = w.Write(image.Body)
		if err != nil {
			return http.ErrAbortHandler
		}
		return nil
	}

	image, condition, err := parseSnapshotPublication(r, projection)
	if err != nil {
		return err
	}
	image, created, err := backend.PutSnapshot(r.Context(), path, image, condition)
	if err != nil {
		return snapshotHTTPError(err)
	}
	h.snapshotHeaders(w, image.Incarnation)
	w.Header().Set(protocol.HeaderStreamSnapshotOffset, image.Offset.String())
	w.Header().Set("ETag", image.ETag)
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	w.WriteHeader(status)
	return nil
}

func parseSnapshotPublication(r *http.Request, projection string) (store.ProjectionSnapshot, store.SnapshotCondition, error) {
	var image store.ProjectionSnapshot
	var condition store.SnapshotCondition
	values := make(map[string]string)
	for _, name := range []string{
		protocol.HeaderStreamSnapshot, protocol.HeaderIfStreamIncarnation,
		protocol.HeaderStreamSnapshotOffset, "Content-Type", protocol.HeaderContentDigest,
	} {
		value, err := snapshotHeader(r, name, true)
		if err != nil {
			return image, condition, err
		}
		values[name] = value
	}
	if values[protocol.HeaderStreamSnapshot] != "v1" {
		return image, condition, newHTTPError(http.StatusBadRequest, "unsupported snapshot version")
	}
	if strings.ContainsAny(values[protocol.HeaderIfStreamIncarnation], " ,\t\r\n") {
		return image, condition, newHTTPError(http.StatusBadRequest, "invalid stream incarnation")
	}
	offset, err := store.ParseOffset(values[protocol.HeaderStreamSnapshotOffset])
	if err != nil || offset.IsNow() || offset.String() != values[protocol.HeaderStreamSnapshotOffset] {
		return image, condition, newHTTPError(http.StatusBadRequest, "snapshot offset must be a concrete canonical next-read offset")
	}
	if _, _, err := mime.ParseMediaType(values["Content-Type"]); err != nil {
		return image, condition, newHTTPError(http.StatusUnsupportedMediaType, "invalid snapshot content type")
	}
	encoding, err := snapshotHeader(r, "Content-Encoding", false)
	if err != nil {
		return image, condition, err
	}
	if encoding != "" && encoding != "identity" {
		return image, condition, newHTTPError(http.StatusUnsupportedMediaType, "snapshot content encoding must be identity")
	}
	match, err := snapshotHeader(r, "If-Match", false)
	if err != nil {
		return image, condition, err
	}
	none, err := snapshotHeader(r, "If-None-Match", false)
	if err != nil {
		return image, condition, err
	}
	if match == "" && none == "" {
		return image, condition, newHTTPError(http.StatusPreconditionRequired, "snapshot publication requires If-Match or If-None-Match")
	}
	if (match != "" && none != "") || (none != "" && none != "*") || (match != "" && !strongSnapshotETag(match)) {
		return image, condition, newHTTPError(http.StatusBadRequest, "provide one strong If-Match ETag or If-None-Match: *")
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, store.MaxSnapshotBytes+1))
	if err != nil {
		return image, condition, newHTTPError(http.StatusBadRequest, "could not read complete snapshot body")
	}
	if len(body) > store.MaxSnapshotBytes {
		return image, condition, newHTTPError(http.StatusRequestEntityTooLarge, "snapshot exceeds maximum size")
	}
	digest := values[protocol.HeaderContentDigest]
	if !strings.HasPrefix(digest, "sha-256=:") || !strings.HasSuffix(digest, ":") {
		return image, condition, newHTTPError(http.StatusBadRequest, "Content-Digest must contain sha-256")
	}
	decoded, err := base64.StdEncoding.Strict().DecodeString(strings.TrimSuffix(strings.TrimPrefix(digest, "sha-256=:"), ":"))
	expected := sha256.Sum256(body)
	if err != nil || !bytes.Equal(decoded, expected[:]) {
		return image, condition, newHTTPError(http.StatusBadRequest, "snapshot content digest mismatch")
	}
	return store.ProjectionSnapshot{
		Projection: projection, Incarnation: values[protocol.HeaderIfStreamIncarnation], Offset: offset,
		ContentType: values["Content-Type"], Body: body,
	}, store.SnapshotCondition{IfMatch: match, IfNoneMatch: none == "*"}, nil
}

func snapshotHeader(r *http.Request, name string, required bool) (string, error) {
	values := r.Header.Values(name)
	if len(values) == 0 && !required {
		return "", nil
	}
	if len(values) != 1 || values[0] == "" {
		return "", newHTTPError(http.StatusBadRequest, "expected one nonempty "+name+" header")
	}
	return values[0], nil
}

func strongSnapshotETag(value string) bool {
	if len(value) < 2 || value[0] != '"' || value[len(value)-1] != '"' {
		return false
	}
	for _, b := range []byte(value[1 : len(value)-1]) {
		if b < 0x21 || b == '"' || b == 0x7f {
			return false
		}
	}
	return true
}

func snapshotDigest(body []byte) string {
	sum := sha256.Sum256(body)
	return "sha-256=:" + base64.StdEncoding.EncodeToString(sum[:]) + ":"
}

func snapshotHTTPError(err error) error {
	switch {
	case errors.Is(err, store.ErrSnapshotNotFound), errors.Is(err, store.ErrStreamNotFound):
		return newHTTPError(http.StatusNotFound, "snapshot or stream not found")
	case errors.Is(err, store.ErrStreamSoftDeleted):
		return newHTTPError(http.StatusGone, "stream has been deleted")
	case errors.Is(err, store.ErrSnapshotPrecondition), errors.Is(err, store.ErrReadSnapshotChanged):
		return newHTTPError(http.StatusPreconditionFailed, "snapshot or stream incarnation changed")
	case errors.Is(err, store.ErrSnapshotConflict):
		return newHTTPError(http.StatusConflict, "snapshot offset or state conflicts with source or saved image")
	case errors.Is(err, store.ErrSnapshotTooLarge):
		return newHTTPError(http.StatusRequestEntityTooLarge, err.Error())
	case errors.Is(err, store.ErrSnapshotQuota):
		return newHTTPError(http.StatusInsufficientStorage, err.Error())
	case errors.Is(err, store.ErrInvalidSnapshot):
		return newHTTPError(http.StatusBadRequest, err.Error())
	default:
		return err
	}
}
