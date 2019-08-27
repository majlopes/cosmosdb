using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Remoting.Messaging;
using System.Text;
using System.Threading;

namespace CosmosGettingStarted
{
    public class Position
    {

        double Lat { get; set; }
        double Lon { get; set; }

        public Position(double lat, double lon)
        {
            this.Lat = lat;
            this.Lon = lon;
        }
        public static double MinimumDistance(List<Position> positions, Position start = null, Position end = null)
        {
            if (start == null)
            {
                start = new Position(0, 0);
            }

            if (end == null && positions != null && positions.Count > 0)
            {
                end = positions.Last();
            }

            double firstCatSquare = Math.Abs(end.Lat - start.Lat) * Math.Abs(end.Lat - start.Lat);
            double secondCatSquare = Math.Abs(end.Lon - start.Lon) * Math.Abs(end.Lon - start.Lon);

            return Math.Sqrt(firstCatSquare + secondCatSquare);
        }

        public static List<int> SetDistance(List<int> totalDistance, int distance)
        {
            //add distance
            totalDistance.Add(distance);
            return totalDistance;
        }

        public static List<Position> SetPosition(List<Position> positions, int distance, int angle, int prevAngle)
        {
            //set distance progress for each axis
            double xAxis = Math.Cos((prevAngle + angle) * Math.PI / 180) * distance;
            double yAxis = Math.Sin((prevAngle + angle) * Math.PI / 180) * distance;

            //add new position
            Position pos = new Position(xAxis, yAxis);
            positions.Add(pos);

            return positions;
        }

    }
}
