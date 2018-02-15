precision highp float;

uniform sampler2D texture;
varying vec2 coord;


void main() {

	vec4 color = texture2D(texture, coord);
	if (color.a < 1.0) {
		color.r = color.r;
		color.g = color.g;
		color.b = color.b;
		color.a = color.a;
	}
	gl_FragColor = color;
}