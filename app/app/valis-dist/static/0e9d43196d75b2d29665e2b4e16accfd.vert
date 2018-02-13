precision mediump float;

attribute vec2 points;
attribute vec4 color;
varying vec4 fragColor;

vec2 UVtoGLCoordinates(vec2 uv) {
	return (uv * 2.0 -  1.0) / vec2(1, -1);
}

vec2 GLtoUVCoordinates(vec2 pt) {
	return (pt * vec2(1, -1) + 1.0) / 2.0; 
}

void main() {
	fragColor = color;
    gl_Position = vec4(UVtoGLCoordinates(points), 0.0, 1.0);
}