precision mediump float;

varying vec2 coord;

uniform vec3 color1;
uniform vec3 color2;
uniform vec3 color3;
uniform vec3 color4;

uniform vec2 displayedRange; // range of genome currently displayed
uniform vec2 totalRange; // total range of the genome
uniform vec2 currentTileDisplayRange; // range of the tile that is displayed
uniform vec2 totalTileRange; // total range of the current tile

uniform vec2 windowSize;
uniform vec2 selectionBoundsMin;
uniform vec2 selectionBoundsMax;
uniform int showSelection;
uniform int isApproximate;
uniform int dimensions;

uniform sampler2D data;

uniform float dataMin;
uniform float dataMax;

uniform float tile;
uniform float selectedBasePair;

#define TICK_WIDTH 2500000.0
#define SMALL_TICK_HEIGHT 0.1

bool gridVisible(float currBp, float pixelsPerBp, float spacing, float thickness, float minSize) {
	float d = fract(currBp/spacing);
	float alpha = thickness / pixelsPerBp  / spacing;
	return ( (d > (1.0 - alpha) || d < alpha) && pixelsPerBp * spacing > minSize);
}

void main() {

	float currBp = mix(currentTileDisplayRange.x, currentTileDisplayRange.y, coord.x);
	float locInTile = (currBp - totalTileRange.x) / (totalTileRange.y - totalTileRange.x);

	// load and normalize data value:
	vec4 rawData = texture2D(data, vec2(locInTile, 0.0));

	float minValue = 0.0;
	int minIdx = -1;
	for (int i = 0; i < 4; i++) {
		if (i >= dimensions) break;
		float d = rawData[i];
		d = (d - dataMin) / (dataMax - dataMin);
		if ((minIdx < 0 || d < minValue) && (1.0 - coord.y) < d) {
			minValue = d;
			minIdx = i;
		}
	}
	

	float bpPerPixel = (displayedRange.y - displayedRange.x) / windowSize.x;
	
	vec3 finalColor = vec3(0.0);
	if ((1.0 - coord.y) < minValue) {
		if (minIdx == 0) {
			finalColor = color1;
		} else if (minIdx == 1) {
			finalColor = color2;
		} else if (minIdx == 2) {
			finalColor = color3;
		} else if (minIdx == 3) {
			finalColor = color4;
		}
	}
	finalColor *= 1.0/255.0;

	vec3 tintColor = vec3(0.0);
	float pixelsPerBp = windowSize.x/(displayedRange.y - displayedRange.x);
	
	if (abs(selectedBasePair - currBp) < 1.0  / pixelsPerBp) {
		tintColor += vec3(0.5, 0.5, 0.5);
	}

	vec2 bMin = vec2(min(selectionBoundsMin.x, selectionBoundsMax.x), min(selectionBoundsMin.y, selectionBoundsMax.y));
	vec2 bMax = vec2(max(selectionBoundsMin.x, selectionBoundsMax.x), max(selectionBoundsMin.y, selectionBoundsMax.y));

	vec2 screenCoord =  gl_FragCoord.xy;
	screenCoord.y = windowSize.y - screenCoord.y;

	vec3 selectionHighlight = vec3(0.0);
	if (showSelection == 1 && screenCoord.x > bMin.x && screenCoord.x < bMax.x) {
		selectionHighlight = vec3(0.3);
	}

	gl_FragColor = vec4(tintColor + finalColor + selectionHighlight, 1.0);	
}