using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CosmosGettingStarted
{
    class Marker
    {
        public string Name;
        public Position MarkerPosition;

        public double DistanceTravelledUntilMarker;


        public Marker(string name, Position markerPosition, double distanceTravelledUntilMarker)
        {
            this.Name = name;
            this.MarkerPosition = markerPosition;
            this.DistanceTravelledUntilMarker = distanceTravelledUntilMarker;
        }
    }
}
