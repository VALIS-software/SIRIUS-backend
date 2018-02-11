precision mediump float;

attribute vec2 points;
varying vec2 coord;
uniform float tileHeight;
uniform vec2 windowSize;
uniform vec2 offset;

uniform vec2 displayedRange;
uniform vec2 totalRange;

uniform vec2 currentTileDisplayRange; // range of the current tile

vec2 UVtoGLCoordinates(vec2 uv) {
	return (uv * 2.0 -  1.0) / vec2(1, -1);
}

vec2 GLtoUVCoordinates(vec2 pt) {
	return (pt * vec2(1, -1) + 1.0) / 2.0; 
}

void main() {
	// coord maps to the range [0, 1] and is sent to the frag shader
    coord = GLtoUVCoordinates(points);
    
    // shrink the ROI to just the current tile:
    coord.x *= (currentTileDisplayRange.y - currentTileDisplayRange.x) / (displayedRange.y - displayedRange.x);
    coord.x += (currentTileDisplayRange.x - displayedRange.x) / (displayedRange.y - displayedRange.x);
    vec2 transformed = coord * vec2(1.0, tileHeight) + vec2(offset.x, offset.y);

    coord = GLtoUVCoordinates(points);
    gl_Position = vec4(UVtoGLCoordinates(transformed), 0.0, 1.0);
}